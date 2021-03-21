use crate::prelude::*;
use chrono::prelude::*;
use chrono::Duration;
use fxdb::*;
use lazy_static::lazy_static;
use std::collections::VecDeque;
use std::env;

// ETH-PERP
pub(crate) static TICK: f64 = 0.1;
pub(crate) static MIN_ORDER: f64 = 0.001;

lazy_static! {
  static ref ORDER_INTERVAL: Duration = Duration::minutes(1);
  static ref BASE_LINE: Duration = Duration::minutes(26);
  static ref CONVERSION_LINE: Duration = Duration::minutes(9);
  static ref SPAN: Duration = Duration::minutes(52);
  static ref OFFSET: Duration = Duration::minutes(26);
  static ref BUCKET_SIZE: Duration = *SPAN + *OFFSET;
  static ref CHART_SIZE: Duration = Duration::minutes(100);
  static ref CANDLE_SIZE: Duration = Duration::minutes(5);
}

pub(crate) struct Strategy {
  bucket: Bucket,
  chart: Chart,
  position: Position,
  last_order: DateTime<Utc>,
  last_cloud: Option<Cloud>,
  exit_order: String,
  losscut_stop: String,
}

impl Strategy {
  pub(crate) async fn new(exchange: &Box<dyn Exchange>) -> Self {
    let trades = exchange.trades(Utc::now() - *CHART_SIZE).await.unwrap();
    let mut bucket = Bucket::new();
    let mut chart = Chart::new();
    for trade in trades.into_iter() {
      bucket.push(trade);
      chart.push(trade);
    }

    let position = exchange.position().await.unwrap();
    let exit_order = exchange
      .orders()
      .await
      .unwrap()
      .into_iter()
      .next()
      .map(|order| order.id)
      .unwrap_or_default();
    let losscut_stop = exchange
      .stops()
      .await
      .unwrap()
      .into_iter()
      .next()
      .map(|stop| stop.id)
      .unwrap_or_default();

    Self {
      bucket,
      chart,
      position,
      last_order: Utc.timestamp(0, 0),
      last_cloud: None,
      exit_order,
      losscut_stop,
    }
  }

  pub(crate) async fn handle_trade(&mut self, exchange: &Box<dyn Exchange>, last_trade: Trade) {
    self.bucket.push(last_trade);
    self.chart.push(last_trade);
    if !self.bucket.is_full || !self.chart.is_full {
      return;
    }

    let base_line = self.bucket.median(
      last_trade.timestamp - *BASE_LINE - *OFFSET,
      last_trade.timestamp - *OFFSET,
    );
    let conversion_line = self.bucket.median(
      last_trade.timestamp - *CONVERSION_LINE - *OFFSET,
      last_trade.timestamp - *OFFSET,
    );
    let span1 = (base_line + conversion_line) / 2.0;
    let span2 = self
      .bucket
      .median(last_trade.timestamp - *SPAN - *OFFSET, last_trade.timestamp - *OFFSET);
    let cloud_top = span1.max(span2);
    let cloud_bottom = span1.min(span2);

    let last_cloud = if let Some(last_cloud) = &mut self.last_cloud {
      last_cloud
    } else if cloud_top > last_trade.price && last_trade.price > cloud_bottom {
      let base_line = self
        .bucket
        .median(last_trade.timestamp - *BASE_LINE, chrono::MAX_DATETIME);
      let conversion_line = self
        .bucket
        .median(last_trade.timestamp - *CONVERSION_LINE, chrono::MAX_DATETIME);
      self.last_cloud = Some(Cloud {
        timestamp: last_trade.timestamp,
        base_line,
        conversion_line,
      });
      return;
    } else {
      return;
    };

    if cloud_top > last_trade.price && last_trade.price > cloud_bottom {
      last_cloud.timestamp = last_trade.timestamp;
      return;
    }

    if last_cloud.timestamp + Duration::minutes(1) > last_trade.timestamp {
      return;
    }

    let (variance, avg) = self.chart.variance();
    let stdev = variance.sqrt();
    let sigma: f64 = env::var("AVENTADOR_SIGMA")
      .expect("Set $AVENTADOR_SIGMA")
      .parse()
      .unwrap();
    let bb_top = ceil_price(avg + stdev * sigma);
    let bb_bottom = floor_price(avg - stdev * sigma);

    if last_cloud.base_line < last_cloud.conversion_line
      && cloud_top < last_trade.price
      && avg + stdev > last_trade.price
      && 0.0 >= self.position.amount
    {
      let qty = self.bet(&exchange).await.unwrap() / self.bucket.best_ask;

      let ((_, _), (_, exit_order, losscut_stop)) = tokio::try_join!(
        self.cancel(&exchange),
        self.entry(&exchange, qty, bb_top, last_trade.price * 2.0 - bb_top)
      )
      .unwrap();
      self.exit_order = exit_order;
      self.losscut_stop = losscut_stop;
      self.last_order = last_trade.timestamp;
    } else if last_cloud.base_line > last_cloud.conversion_line
      && cloud_bottom > last_trade.price
      && avg - stdev < last_trade.price
      && 0.0 <= self.position.amount
    {
      let qty = self.bet(&exchange).await.unwrap() / self.bucket.best_bid;

      let ((_, _), (_, exit_order, losscut_stop)) = tokio::try_join!(
        self.cancel(&exchange),
        self.entry(&exchange, -qty, bb_bottom, last_trade.price * 2.0 - bb_bottom)
      )
      .unwrap();
      self.exit_order = exit_order;
      self.losscut_stop = losscut_stop;
      self.last_order = last_trade.timestamp;
    }

    self.last_cloud = None;
  }

  pub(crate) async fn handle_order(&mut self, exchange: &Box<dyn Exchange>, order: Order) {
    return;
  }

  pub(crate) async fn handle_position(&mut self, exchange: &Box<dyn Exchange>, position: Position) {
    if !position.exists() {
      self.cancel(&exchange).await.unwrap();
    }

    self.position = position;
  }

  async fn entry(
    &self,
    exchange: &Box<dyn Exchange>,
    qty: f64,
    exit_price: f64,
    losscut_price: f64,
  ) -> Result<(String, String, String), Error> {
    tokio::try_join!(
      exchange.market_order(qty - self.position.amount),
      exchange.limit_order(exit_price, -qty),
      exchange.stop_market(losscut_price, -qty)
    )
  }

  async fn cancel(&self, exchange: &Box<dyn Exchange>) -> Result<((), ()), Error> {
    let cancel_order = async move {
      if 0 >= self.exit_order.len() {
        Ok(())
      } else {
        match exchange.cancel_order(&self.exit_order).await {
          Ok(()) | Err(Error::AlreadyCancelled) => Ok(()),
          err => err,
        }
      }
    };
    let cancel_stop = async move {
      if 0 >= self.losscut_stop.len() {
        Ok(())
      } else {
        match exchange.cancel_stop(&self.losscut_stop).await {
          Ok(()) | Err(Error::AlreadyCancelled) => Ok(()),
          err => err,
        }
      }
    };
    tokio::try_join!(cancel_order, cancel_stop)
  }

  async fn bet(&self, exchange: &Box<dyn Exchange>) -> Result<f64, Error> {
    let balance = exchange.balance();
    let leverage = exchange.leverage();
    let (balance, leverage) = tokio::try_join!(balance, leverage).unwrap();
    Ok(balance * leverage * 0.9)
  }
}

fn floor_price(price: f64) -> f64 {
  (price / TICK).floor() * TICK
}

fn ceil_price(price: f64) -> f64 {
  (price / TICK).ceil() * TICK
}

fn ceil_order(amount: f64) -> f64 {
  (amount / MIN_ORDER).ceil() * MIN_ORDER
}

fn floor_order(amount: f64) -> f64 {
  (amount / MIN_ORDER).floor() * MIN_ORDER
}

struct Bucket {
  trades: VecDeque<Trade>,
  is_full: bool,
  best_bid: f64,
  best_ask: f64,
}

impl Bucket {
  fn new() -> Self {
    Bucket {
      trades: VecDeque::new(),
      is_full: false,
      best_bid: 0.0,
      best_ask: 0.0,
    }
  }

  fn push(&mut self, trade: Trade) {
    self.trades.push_back(trade);

    if trade.is_buy() {
      self.best_ask = trade.price;
    } else if trade.is_sell() {
      self.best_bid = trade.price;
    }

    while trade.timestamp - *BUCKET_SIZE >= self.trades.front().unwrap().timestamp {
      self.trades.pop_front();
      self.is_full = true;
    }
  }

  fn median(&self, since: DateTime<Utc>, until: DateTime<Utc>) -> f64 {
    let since_index = self
      .trades
      .iter()
      .position(|trade| since <= trade.timestamp)
      .unwrap_or(0);
    let until_index = self
      .trades
      .iter()
      .rposition(|trade| trade.timestamp < until)
      .unwrap_or(usize::MAX);
    let length = if since_index < until_index {
      until_index - since_index
    } else {
      1
    };
    let max = self
      .trades
      .iter()
      .skip(since_index)
      .take(length)
      .fold(f64::NAN, |acc, x| acc.max(x.price));
    let min = self
      .trades
      .iter()
      .skip(since_index)
      .take(length)
      .fold(f64::NAN, |acc, x| acc.min(x.price));
    (min + max) / 2.0
  }
}

struct Candle {
  timestamp: DateTime<Utc>,
  open: f64,
  high: f64,
  low: f64,
  close: f64,
  buy: f64,
  sell: f64,
}

struct Chart {
  candles: VecDeque<Candle>,
  is_full: bool,
}

impl Chart {
  fn new() -> Self {
    Self {
      candles: VecDeque::new(),
      is_full: true,
    }
  }

  fn push(&mut self, trade: Trade) {
    if 0 >= self.candles.len() {
      let open_at = trade.timestamp.timestamp() - trade.timestamp.timestamp() % CANDLE_SIZE.num_seconds();
      let candle = Candle {
        timestamp: Utc.timestamp(open_at, 0),
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        buy: 0.0,
        sell: 0.0,
      };
      self.candles.push_back(candle);
    }

    while self.candles.back().unwrap().timestamp + *CANDLE_SIZE <= trade.timestamp {
      let candle = Candle {
        timestamp: self.candles.back().unwrap().timestamp + *CANDLE_SIZE,
        open: self.candles.back().unwrap().close,
        high: self.candles.back().unwrap().close,
        low: self.candles.back().unwrap().close,
        close: self.candles.back().unwrap().close,
        buy: 0.0,
        sell: 0.0,
      };
      self.candles.push_back(candle);
    }

    self.candles.back_mut().unwrap().close = trade.price;

    if self.candles.back().unwrap().high < trade.price {
      self.candles.back_mut().unwrap().high = trade.price;
    }

    if self.candles.back().unwrap().low > trade.price {
      self.candles.back_mut().unwrap().low = trade.price;
    }

    if 0.0 < trade.amount {
      self.candles.back_mut().unwrap().buy += trade.amount;
    }

    if 0.0 > trade.amount {
      self.candles.back_mut().unwrap().sell -= trade.amount;
    }

    while self.candles.back().unwrap().timestamp - *CHART_SIZE > self.candles.front().unwrap().timestamp {
      self.candles.pop_front();
    }
  }

  fn variance(&self) -> (f64, f64) {
    let avg = self.candles.iter().map(|candle| candle.close).sum::<f64>() / self.candles.len() as f64;
    let variance = self
      .candles
      .iter()
      .map(|candle| (candle.close - avg).powi(2))
      .sum::<f64>()
      / self.candles.len() as f64;
    (variance, avg)
  }
}

#[derive(Debug)]
struct Cloud {
  timestamp: DateTime<Utc>,
  base_line: f64,
  conversion_line: f64,
}
