use crate::prelude::*;
use async_trait::async_trait;
use chrono::prelude::*;
use chrono::Duration;
use futures::prelude::*;
use futures::task::{Context, Poll};
use fxdb::*;
use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, Request, Url};
use ring::hmac;
use ring::hmac::{Key, HMAC_SHA256};
use serde_json::{json, Value};
use std::convert::TryInto;
use std::env;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::pin;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

lazy_static! {
  static ref SECRET: Key = Key::new(HMAC_SHA256, env::var("FTX_SECRET").unwrap().as_bytes());
}

pub(crate) struct Ftx {
  http: Client,
  ws: Connection,
  market: String,
  position: Position,
}

impl Ftx {
  pub(crate) async fn new(market: String) -> Self {
    let mut headers = HeaderMap::new();
    headers.insert(
      HeaderName::from_static("content-type"),
      HeaderValue::from_static("application/json"),
    );
    let http = Client::builder()
      .default_headers(headers)
      .timeout(Duration::seconds(10).to_std().unwrap())
      .build()
      .unwrap();
    let ws = Self::connect().await;

    let mut itself = Self {
      http,
      ws,
      market,
      position: Position::neutral(),
    };
    itself.position = itself.position().await.unwrap();
    itself
  }

  fn sign(req: &mut Request) {
    req
      .headers_mut()
      .append("FTX-KEY", env::var("FTX_KEY").unwrap().try_into().unwrap());
    req.headers_mut().append(
      "FTX-SUBACCOUNT",
      env::var("FTX_SUBACCOUNT").unwrap().try_into().unwrap(),
    );

    let timestamp = Utc::now().timestamp_millis();
    req.headers_mut().append("FTX-TS", timestamp.into());

    let preimage = format!(
      "{}{}{}{}",
      timestamp,
      req.method(),
      &req.url()[url::Position::AfterPort..],
      std::str::from_utf8(req.body().and_then(|body| body.as_bytes()).unwrap_or(&[])).unwrap()
    );
    let signature = hex::encode(hmac::sign(&SECRET, preimage.as_ref()));
    req.headers_mut().append("FTX-SIGN", signature.try_into().unwrap());
  }

  async fn fetch(&self, method: Method, path: &str, params: Value) -> Result<Value, Error> {
    let mut url = Url::parse("https://ftx.com/api").unwrap();
    for path in path.split("/") {
      if 0 < path.len() {
        url.path_segments_mut().unwrap().push(path);
      }
    }

    let mut request = match method {
      Method::GET if !params.is_null() => self.http.request(method, url).query(&params).build().unwrap(),
      Method::POST | Method::DELETE if !params.is_null() => {
        self.http.request(method, url).json(&params).build().unwrap()
      }
      _ => self.http.request(method, url).build().unwrap(),
    };
    Self::sign(&mut request);

    log::trace!("HTTP request sent: {:?}", request);
    if let Some(body) = request.body() {
      log::trace!(
        "HTTP request JSON: {}",
        std::str::from_utf8(body.as_bytes().unwrap()).unwrap()
      );
    }

    let resp = self.http.execute(request).await.unwrap();
    log::trace!("HTTP response came: {:?}", resp);
    let status_code = resp.status();
    let json: Value = resp.json().await.unwrap();
    log::trace!("HTTP response JSON: {}", json);

    if status_code.is_success() {
      return Ok(json);
    }

    match json.get("error").and_then(|json| json.as_str()) {
      Some("Account does not have enough margin for order.") => Err(Error::InsufficientBalance),
      Some("Order already closed") => Err(Error::AlreadyCancelled),
      _ => Err(Error::Unknown),
    }
  }

  async fn connect() -> Connection {
    let url = Url::parse("wss://ftx.com/ws").unwrap();
    let (mut conn, _) = connect_async(url).await.unwrap();

    let timestamp = Utc::now().timestamp_millis();
    let preimage = format!("{}websocket_login", timestamp);
    let signature = hex::encode(hmac::sign(&SECRET, preimage.as_ref()));
    let payload = json!({
      "op": "login",
      "args": {
        "key": env::var("FTX_KEY").unwrap(),
        "sign": signature,
        "time": timestamp,
        "subaccount": env::var("FTX_SUBACCOUNT").unwrap()
      }
    });
    conn.send(Message::Text(payload.to_string())).await.unwrap();

    conn
      .inspect(log_incoming as fn(&Result<Message, tungstenite::Error>))
      .with(log_outgoing)
  }

  pub(crate) async fn subscribe(&mut self, topic: Topic) {
    let channel = match topic {
      Topic::Trade => "trades",
      Topic::Order => "orders",
      Topic::Position => "fills",
    };

    let payload = json!({
      "op": "subscribe",
      "channel": channel,
      "market": self.market
    });
    self.ws.send(Ok(Message::Text(payload.to_string()))).await.unwrap();

    while let Some(Ok(msg)) = self.ws.next().await {
      let json: Value = if let Message::Text(text) = msg {
        text.parse().unwrap()
      } else {
        continue;
      };

      if json.get("type").and_then(|field| field.as_str()) == Some("subscribed")
        && json.get("channel").and_then(|field| field.as_str()) == Some(channel)
      {
        return;
      }
    }

    panic!("WebSocket connection closed");
  }
}

#[async_trait(?Send)]
impl Exchange for Ftx {
  async fn limit_order(&self, price: f64, amount: f64) -> Result<String, Error> {
    let side = if 0.0 < amount {
      "buy"
    } else if 0.0 > amount {
      "sell"
    } else {
      unreachable!();
    };

    let resp = self
      .fetch(
        Method::POST,
        "/orders",
        json!({
          "market": self.market,
          "side": side,
          "price": price,
          "type": "limit",
          "size": amount.abs(),
          "postOnly": true,
        }),
      )
      .await?;

    Ok(resp["result"]["id"].as_u64().unwrap().to_string())
  }

  async fn market_order(&self, amount: f64) -> Result<String, Error> {
    let side = if 0.0 < amount {
      "buy"
    } else if 0.0 > amount {
      "sell"
    } else {
      unreachable!();
    };

    let resp = self
      .fetch(
        Method::POST,
        "/orders",
        json!({
          "market": self.market,
          "side": side,
          "price": null,
          "type": "market",
          "size": amount.abs(),
        }),
      )
      .await?;

    Ok(resp["result"]["id"].as_u64().unwrap().to_string())
  }

  async fn cancel_order(&self, order_id: &str) -> Result<(), Error> {
    self
      .fetch(Method::DELETE, &format!("/orders/{}", order_id), Value::Null)
      .await?;
    Ok(())
  }

  async fn cancel_orders(&self) -> Result<(), Error> {
    self
      .fetch(
        Method::DELETE,
        "/orders",
        json!({
          "market": self.market
        }),
      )
      .await?;
    Ok(())
  }

  async fn stop_market(&self, trigger: f64, amount: f64) -> Result<String, Error> {
    let side = if 0.0 < amount {
      "buy"
    } else if 0.0 > amount {
      "sell"
    } else {
      unreachable!()
    };

    let resp = self
      .fetch(
        Method::POST,
        "/conditional_orders",
        json!({
          "market": self.market,
          "side": side,
          "type": "stop",
          "size": amount.abs(),
          "reduceOnly": true,
          "triggerPrice": trigger
        }),
      )
      .await?;

    Ok(resp["result"]["id"].as_u64().unwrap().to_string())
  }

  async fn stop_limit(&self, trigger: f64, price: f64, amount: f64) -> Result<String, Error> {
    let side = if 0.0 < amount {
      "buy"
    } else if 0.0 > amount {
      "sell"
    } else {
      unreachable!()
    };

    let resp = self
      .fetch(
        Method::POST,
        "/conditional_orders",
        json!({
          "market": self.market,
          "side": side,
          "type": "take_profit",
          "size": amount.abs(),
          "reduceOnly": true,
          "triggerPrice": trigger,
          "orderPrice": price
        }),
      )
      .await?;

    Ok(resp["result"]["id"].as_u64().unwrap().to_string())
  }

  async fn cancel_stop(&self, stop_id: &str) -> Result<(), Error> {
    self
      .fetch(Method::DELETE, &format!("/conditional_orders/{}", stop_id), Value::Null)
      .await?;
    Ok(())
  }

  async fn cancel_stops(&self) -> Result<(), Error> {
    self.cancel_orders().await
  }

  async fn orders(&self) -> Result<Vec<Order>, Error> {
    let resp = self
      .fetch(
        Method::GET,
        "/orders",
        json!({
          "market": self.market
        }),
      )
      .await?;
    let orders = resp["result"]
      .as_array()
      .unwrap()
      .into_iter()
      .filter(|json| json["type"].as_str().unwrap() == "limit" && json["status"].as_str().unwrap() == "open")
      .map(|json| Order {
        id: json["id"].as_u64().unwrap().to_string(),
        price: Some(json["price"].as_f64().unwrap()),
        amount: match json["side"].as_str().unwrap() {
          "buy" => 1.0,
          "sell" => -1.0,
          _ => unreachable!(),
        } * json["remainingSize"].as_f64().unwrap(),
        filled: match json["side"].as_str().unwrap() {
          "buy" => 1.0,
          "sell" => -1.0,
          _ => unreachable!(),
        } * json["filledSize"].as_f64().unwrap(),
      })
      .collect();
    Ok(orders)
  }

  async fn stops(&self) -> Result<Vec<Stop>, Error> {
    let resp = self
      .fetch(
        Method::GET,
        "/conditional_orders",
        json!({
          "market": self.market
        }),
      )
      .await?;
    let stops = resp["result"]
      .as_array()
      .unwrap()
      .into_iter()
      .filter(|json| json["status"].as_str().unwrap() == "open")
      .map(|json| Stop {
        id: json["id"].as_u64().unwrap().to_string(),
        trigger: json["triggerPrice"].as_f64().unwrap(),
        price: json.get("orderPrice").and_then(|json| json.as_f64()),
        amount: match json["side"].as_str().unwrap() {
          "buy" => 1.0,
          "sell" => -1.0,
          _ => unreachable!(),
        } * json["size"].as_f64().unwrap(),
      })
      .collect();
    Ok(stops)
  }

  async fn trades(&self, since: DateTime<Utc>) -> Result<Vec<Trade>, Error> {
    let page_length = 100;
    let mut end_time = Utc::now();
    let mut result: Vec<Trade> = Vec::with_capacity(page_length);

    while since < end_time {
      let mut resp = self
        .fetch(
          Method::GET,
          &format!("/markets/{}/trades", self.market),
          json!({
            "limit": page_length,
            "end_time": end_time.timestamp()
          }),
        )
        .await?;

      let trades = resp["result"].as_array_mut().unwrap();
      for json in trades {
        let trade = Trade {
          timestamp: json["time"].as_str().unwrap().parse().unwrap(),
          price: json["price"].as_f64().unwrap(),
          amount: match json["side"].as_str().unwrap() {
            "buy" => 1.0,
            "sell" => -1.0,
            _ => unreachable!(),
          } * json["size"].as_f64().unwrap(),
        };
        end_time = trade.timestamp;
        result.push(trade);
      }
    }

    result.reverse();
    Ok(result)
  }

  async fn position(&self) -> Result<Position, Error> {
    let resp = self.fetch(Method::GET, "/account", Value::Null).await?;
    let leverage = resp["result"]["leverage"].as_f64().unwrap();
    if let Some(json) = resp["result"]["positions"]
      .as_array()
      .unwrap()
      .iter()
      .find(|json| json["future"] == self.market)
    {
      Ok(Position {
        price: json["entryPrice"].as_f64().unwrap_or(0.0),
        amount: match json["side"].as_str().unwrap() {
          "buy" => 1.0,
          "sell" => -1.0,
          _ => unreachable!(),
        } * json["size"].as_f64().unwrap(),
        leverage,
        is_liquidated: false,
      })
    } else {
      let mut position = Position::neutral();
      position.leverage = leverage;
      Ok(position)
    }
  }

  async fn balance(&self) -> Result<f64, Error> {
    let resp = self.fetch(Method::GET, "/account", Value::Null).await?;
    Ok(resp["result"]["collateral"].as_f64().unwrap())
  }

  async fn leverage(&self) -> Result<f64, Error> {
    let resp = self.fetch(Method::GET, "/account", Value::Null).await?;
    Ok(resp["result"]["leverage"].as_f64().unwrap())
  }
}

impl Stream for Ftx {
  type Item = Incoming;

  fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
    let itself = self.get_mut();
    let ws = &mut itself.ws;
    pin!(ws);

    let msg = match ws.poll_next(ctx) {
      Poll::Ready(Some(Ok(msg))) => msg,
      Poll::Ready(Some(Err(err))) => {
        log::error!("WebSocket connection closed: {:?}", err);
        return Poll::Ready(None);
      }
      Poll::Ready(None) => return Poll::Ready(None),
      Poll::Pending => return Poll::Pending,
    };
    let json: Value = if let Message::Text(text) = msg {
      text.parse().unwrap()
    } else {
      return Poll::Pending;
    };
    if json["type"].as_str().unwrap() != "update" {
      return Poll::Pending;
    }

    if let Some(market) = json.get("market") {
      if market.as_str().unwrap() != itself.market {
        return Poll::Pending;
      }
    }

    let channel = json.get("channel").and_then(|field| field.as_str()).unwrap_or("");
    match channel {
      "trades" => {
        let trades: Vec<Trade> = json["data"]
          .as_array()
          .unwrap()
          .into_iter()
          .map(|json| Trade {
            timestamp: json["time"].as_str().unwrap().parse().unwrap(),
            price: json["price"].as_f64().unwrap(),
            amount: match json["side"].as_str().unwrap() {
              "buy" => 1.0,
              "sell" => -1.0,
              _ => unreachable!(),
            } * json["size"].as_f64().unwrap(),
          })
          .collect();

        Poll::Ready(Some(Incoming::Trade(trades)))
      }
      "fills" => {
        let exec_price = json["data"]["price"].as_f64().unwrap();
        let exec_amount = match json["data"]["side"].as_str().unwrap() {
          "buy" => 1.0,
          "sell" => -1.0,
          _ => unreachable!(),
        } * json["data"]["size"].as_f64().unwrap();

        if itself.position.is_buy() && itself.position.amount <= -exec_amount
          || itself.position.is_sell() && -itself.position.amount <= exec_amount
          || !itself.position.exists() && exec_amount != 0.0
        {
          itself.position.price = exec_price
        } else if itself.position.is_buy() && 0.0 < exec_amount || itself.position.is_sell() && 0.0 > exec_amount {
          itself.position.price = (exec_price * exec_amount.abs()
            + itself.position.price * itself.position.amount.abs())
            / (exec_amount.abs() + itself.position.amount.abs());
        }

        itself.position.amount = ((itself.position.amount + exec_amount) / MIN_ORDER).round() * MIN_ORDER;
        log::trace!("{:?}", itself.position);
        Poll::Ready(Some(Incoming::Position(vec![itself.position])))
      }
      "orders" => {
        let order = Order {
          id: json["data"]["id"].to_string(),
          price: if json["data"]["type"] == "limit" {
            Some(json["data"]["price"].as_f64().unwrap())
          } else {
            None
          },
          amount: match json["data"]["side"].as_str().unwrap() {
            "buy" => 1.0,
            "sell" => -1.0,
            _ => unreachable!(),
          } * json["data"]["remainingSize"].as_f64().unwrap(),
          filled: match json["data"]["side"].as_str().unwrap() {
            "buy" => 1.0,
            "sell" => -1.0,
            _ => unreachable!(),
          } * json["data"]["filledSize"].as_f64().unwrap(),
        };
        Poll::Ready(Some(Incoming::Order(vec![order])))
      }
      _ => Poll::Pending,
    }
  }
}

impl Sink<()> for Ftx {
  type Error = tungstenite::Error;

  fn poll_ready(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    let ws = &mut self.get_mut().ws;
    pin!(ws);
    ws.poll_ready(ctx)
  }

  fn start_send(self: Pin<&mut Self>, item: ()) -> Result<(), Self::Error> {
    let ws = &mut self.get_mut().ws;
    pin!(ws);
    ws.start_send(Ok(Message::Text(json!({"op": "ping"}).to_string())))
  }

  fn poll_flush(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    let ws = &mut self.get_mut().ws;
    pin!(ws);
    ws.poll_flush(ctx)
  }

  fn poll_close(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    let ws = &mut self.get_mut().ws;
    pin!(ws);
    ws.poll_close(ctx)
  }
}

fn log_incoming(msg: &Result<Message, tungstenite::Error>) {
  log::trace!("WebSocket message incoming: {:?}", msg)
}

fn log_outgoing(msg: Result<Message, tungstenite::Error>) -> future::Ready<Result<Message, tungstenite::Error>> {
  log::trace!("WebSocket message outgoing: {:?}", msg);
  future::ready(msg)
}

type Connection = sink::With<
  stream::Inspect<WebSocketStream<MaybeTlsStream<TcpStream>>, fn(&Result<Message, tungstenite::Error>)>,
  tungstenite::Message,
  Result<Message, tungstenite::Error>,
  future::Ready<Result<Message, tungstenite::Error>>,
  fn(Result<Message, tungstenite::Error>) -> future::Ready<Result<Message, tungstenite::Error>>,
>;
