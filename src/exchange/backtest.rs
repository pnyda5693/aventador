use crate::prelude::*;
use async_trait::async_trait;
use chrono::prelude::*;
use chrono::Duration;
use futures::prelude::*;
use fxdb::*;
use lazy_static::lazy_static;
use plotters::prelude::*;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::Drop;
use std::pin::Pin;
use std::{
  path::PathBuf,
  task::{Context, Poll},
};
use tokio_tungstenite::tungstenite;
use uuid::Uuid;

static LIMIT_FEE: f64 = -0.00 * 0.01;
static MARKET_FEE: f64 = 0.00 * 0.07;

lazy_static! {
  static ref SINCE: DateTime<Utc> = Utc.ymd(2021, 02, 19).and_hms(0, 0, 0);
  static ref UNTIL: DateTime<Utc> = Utc.ymd(2021, 03, 19).and_hms(0, 0, 0);
  static ref LAG: Duration = Duration::seconds(1);
}

struct Inner {
  pnl: f64,
  position: Position,
  orders: Vec<LaggedOrder>,
  stops: Vec<LaggedStop>,
  trades: TradeIter,
  outgoing: VecDeque<Incoming>,
  now: DateTime<Utc>,
  execs: Vec<Execution>,
  best_bid: f64,
  best_ask: f64,
  report: PathBuf,
}

impl Inner {
  fn execute_order(&mut self, order: &mut Order, amount: f64) {
    assert!(0.0 < amount);
    let exec_price = if let Some(price) = order.price {
      price
    } else if order.is_buy() {
      self.best_ask
    } else if order.is_sell() {
      self.best_bid
    } else {
      panic!("order with no amount")
    };
    let exec_amount = if order.price.is_none() {
      order.amount
    } else if order.is_buy() {
      order.amount.min(amount)
    } else if order.is_sell() {
      order.amount.max(-amount)
    } else {
      panic!("order with no amount")
    };

    if self.position.is_buy() && 0.0 > exec_amount {
      self.pnl += self.position.amount.min(-exec_amount) * (exec_price - self.position.price);
    } else if self.position.is_sell() && 0.0 < exec_amount {
      self.pnl += exec_amount.min(-self.position.amount) * (self.position.price - exec_price);
    }

    if self.position.is_buy() && self.position.amount <= -exec_amount
      || self.position.is_sell() && -self.position.amount <= exec_amount
      || !self.position.exists() && exec_amount != 0.0
    {
      self.position.price = exec_price
    } else if self.position.is_buy() && 0.0 < exec_amount || self.position.is_sell() && 0.0 > exec_amount {
      self.position.price = (exec_price * exec_amount.abs() + self.position.price * self.position.amount.abs())
        / (exec_amount.abs() + self.position.amount.abs());
    }

    self.position.amount += exec_amount;
    order.amount -= exec_amount;
    order.filled += exec_amount;

    if order.price.is_some() {
      self.pnl -= exec_amount.abs() * LIMIT_FEE * exec_price;
    } else {
      self.pnl -= exec_amount.abs() * MARKET_FEE * exec_price;
    }

    self.outgoing.push_front(Incoming::Order(vec![order.clone()]));
    let position = self.position;
    self.outgoing.push_front(Incoming::Position(vec![position]));
    let exec = Execution {
      timestamp: self.now,
      price: exec_price,
      position: self.position,
      pnl: self.pnl,
    };
    self.execs.push(exec);

    if exec_amount != 0.0 {
      dbg!(&self.orders);
      dbg!(&self.stops);
      dbg!(self.position);
    }

    println!("{} ${}", self.now.to_rfc3339(), self.pnl);
  }

  fn forward_time(&mut self, trade: Trade) {
    self.now = trade.timestamp;

    if trade.is_buy() {
      self.best_ask = trade.price;
    } else if trade.is_sell() {
      self.best_bid = trade.price;
    }

    self.stops = self
      .stops
      .clone()
      .into_iter()
      .filter_map(|stop| {
        if stop.since > self.now {
          return Some(stop);
        }

        if stop.until <= self.now {
          return None;
        }

        if stop.stop.is_buy() && stop.stop.trigger <= trade.price
          || stop.stop.is_sell() && stop.stop.trigger >= trade.price
        {
          let order = LaggedOrder {
            since: self.now,
            until: chrono::MAX_DATETIME,
            order: Order {
              id: stop.stop.id,
              price: stop.stop.price,
              amount: stop.stop.amount,
              filled: 0.0,
            },
          };
          self.orders.push(order);
          return None;
        }

        Some(stop)
      })
      .collect();

    self.orders = self
      .orders
      .clone()
      .into_iter()
      .filter_map(|mut order| {
        if order.since > self.now {
          return Some(order);
        }

        if order.until <= self.now {
          return None;
        }

        if let Some(price) = order.order.price {
          let max_exec_amount = if trade.is_sell() && order.order.is_buy() && price > trade.price {
            -trade.amount
          } else if trade.is_buy() && order.order.is_sell() && price < trade.price {
            trade.amount
          } else {
            return Some(order);
          };
          self.execute_order(&mut order.order, max_exec_amount);
        } else {
          self.execute_order(&mut order.order, f64::MAX);
        }

        if order.order.amount == 0.0 {
          None
        } else {
          Some(order)
        }
      })
      .collect();
  }
}

pub(crate) struct Backtest(RefCell<Inner>);

impl Backtest {
  pub(crate) fn new() -> Self {
    let filename = std::env::args().skip(1).next().expect("Give me a filename");

    let trades = Trade::fetch(*SINCE, *UNTIL);
    let inner = Inner {
      pnl: 0.0,
      position: Position::neutral(),
      orders: Vec::new(),
      stops: Vec::new(),
      outgoing: VecDeque::new(),
      now: Utc.timestamp(0, 0),
      best_ask: 0.0,
      best_bid: 0.0,
      trades,
      execs: Vec::new(),
      report: PathBuf::from(filename),
    };
    Self(RefCell::new(inner))
  }
}

#[async_trait(?Send)]
impl Exchange for Backtest {
  async fn limit_order(&self, price: f64, amount: f64) -> Result<String, Error> {
    let mut inner = self.0.borrow_mut();

    if 0.0 < amount && inner.best_ask <= price || 0.0 > amount && inner.best_bid >= price {
      panic!("You cannot be taker");
    }

    let id = Uuid::new_v4();
    let order = LaggedOrder {
      since: inner.now + *LAG,
      until: chrono::MAX_DATETIME,
      order: Order {
        id: id.to_string(),
        price: Some(price),
        amount,
        filled: 0.0,
      },
    };
    inner.orders.push(order);
    Ok(id.to_string())
  }

  async fn market_order(&self, amount: f64) -> Result<String, Error> {
    let id = Uuid::new_v4();
    let mut order = Order {
      id: id.to_string(),
      price: None,
      amount,
      filled: 0.0,
    };
    self.0.borrow_mut().execute_order(&mut order, f64::MAX);
    Ok(id.to_string())
  }

  async fn cancel_order(&self, order_id: &str) -> Result<(), Error> {
    let mut inner = self.0.borrow_mut();
    let now = inner.now;

    for order in inner.orders.iter_mut() {
      if order.order.id == order_id {
        order.until = now + *LAG;
        break;
      }
    }

    Ok(())
  }

  async fn cancel_orders(&self) -> Result<(), Error> {
    let mut inner = self.0.borrow_mut();
    let now = inner.now;

    for order in inner.orders.iter_mut() {
      order.until = now + *LAG;
    }

    Ok(())
  }

  async fn stop_market(&self, trigger: f64, amount: f64) -> Result<String, Error> {
    let mut inner = self.0.borrow_mut();

    let id = Uuid::new_v4();
    let stop = LaggedStop {
      since: inner.now + *LAG,
      until: chrono::MAX_DATETIME,
      stop: Stop {
        id: id.to_string(),
        trigger,
        price: None,
        amount,
      },
    };
    inner.stops.push(stop);

    Ok(id.to_string())
  }

  async fn stop_limit(&self, trigger: f64, price: f64, amount: f64) -> Result<String, Error> {
    let mut inner = self.0.borrow_mut();

    let id = Uuid::new_v4();
    let stop = LaggedStop {
      since: inner.now + *LAG,
      until: chrono::MAX_DATETIME,
      stop: Stop {
        id: id.to_string(),
        trigger,
        price: Some(price),
        amount,
      },
    };
    inner.stops.push(stop);

    Ok(id.to_string())
  }

  async fn cancel_stop(&self, stop_id: &str) -> Result<(), Error> {
    let mut inner = self.0.borrow_mut();
    let now = inner.now;

    for stop in inner.stops.iter_mut() {
      if stop.stop.id == stop_id {
        stop.until = now + *LAG;
        break;
      }
    }
    Ok(())
  }

  async fn cancel_stops(&self) -> Result<(), Error> {
    let mut inner = self.0.borrow_mut();
    let now = inner.now;

    for stop in inner.stops.iter_mut() {
      stop.until = now + *LAG;
    }

    Ok(())
  }

  async fn orders(&self) -> Result<Vec<Order>, Error> {
    let inner = self.0.borrow();

    let orders = inner
      .orders
      .iter()
      .filter(|order| order.since <= inner.now && inner.now < order.until)
      .map(|order| order.order.clone())
      .collect();
    Ok(orders)
  }

  async fn stops(&self) -> Result<Vec<Stop>, Error> {
    let inner = self.0.borrow();

    let stops = inner
      .stops
      .iter()
      .filter(|stop| stop.since <= inner.now && inner.now < stop.until)
      .map(|stop| stop.stop.clone())
      .collect();
    Ok(stops)
  }

  async fn trades(&self, since: DateTime<Utc>) -> Result<Vec<Trade>, Error> {
    let mut inner = self.0.borrow_mut();

    Ok(vec![inner.trades.next().expect("No trade fonud")])
  }

  async fn position(&self) -> Result<Position, Error> {
    let inner = self.0.borrow();

    Ok(inner.position)
  }

  async fn balance(&self) -> Result<f64, Error> {
    Ok(0.0)
  }

  async fn leverage(&self) -> Result<f64, Error> {
    Ok(10.0)
  }
}

impl Stream for Backtest {
  type Item = Incoming;

  fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
    let itself = self.get_mut();
    let mut inner = itself.0.borrow_mut();

    if let Some(outgoing) = inner.outgoing.pop_front() {
      ctx.waker().wake_by_ref();
      Poll::Ready(Some(outgoing))
    } else if let Some(trade) = inner.trades.next() {
      inner.forward_time(trade);
      ctx.waker().wake_by_ref();
      Poll::Ready(Some(Incoming::Trade(vec![trade])))
    } else {
      Poll::Ready(None)
    }
  }
}

impl Sink<()> for Backtest {
  type Error = tungstenite::Error;

  fn poll_ready(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    Poll::Ready(Ok(()))
  }

  fn start_send(self: Pin<&mut Self>, item: ()) -> Result<(), Self::Error> {
    Ok(())
  }

  fn poll_flush(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    Poll::Ready(Ok(()))
  }

  fn poll_close(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    Poll::Ready(Ok(()))
  }
}

impl Drop for Inner {
  fn drop(&mut self) {
    let area = BitMapBackend::new(&self.report, (3840, 2160)).into_drawing_area();
    area.fill(&WHITE).unwrap();

    let min_price: f64 = self.execs.iter().fold(f64::NAN, |acc, exec| acc.min(exec.price));
    let max_price: f64 = self.execs.iter().fold(f64::NAN, |acc, exec| acc.max(exec.price));
    let mut chart = ChartBuilder::on(&area)
      .build_ranged(*SINCE..*UNTIL, min_price..max_price)
      .unwrap();
    let series = LineSeries::new(self.execs.iter().map(|exec| (exec.timestamp, exec.price)), &RED);
    chart.draw_series(series).unwrap();

    let min_position: f64 = self
      .execs
      .iter()
      .fold(f64::NAN, |acc, exec| acc.min(exec.position.amount));
    println!("最低ポジション: ₿ {}", min_position);
    let max_position: f64 = self
      .execs
      .iter()
      .fold(f64::NAN, |acc, exec| acc.max(exec.position.amount));
    println!("最大ポジション: ₿ {}", max_position);
    let mut chart = ChartBuilder::on(&area)
      .build_ranged(*SINCE..*UNTIL, min_position..max_position)
      .unwrap();
    let series = LineSeries::new(
      self.execs.iter().map(|exec| (exec.timestamp, exec.position.amount)),
      &GREEN,
    );
    chart.draw_series(series).unwrap();
    let series = LineSeries::new(vec![(*SINCE, 0.0), (*UNTIL, 0.0)], &GREEN);
    chart.draw_series(series).unwrap();

    let mut execs: Vec<Execution> = Vec::with_capacity(self.execs.len());
    execs.push(self.execs[0]);
    for (prev_exec, next_exec) in self.execs.iter().zip(self.execs.iter().skip(1)) {
      if prev_exec.pnl != next_exec.pnl {
        execs.push(*next_exec);
      }
    }
    self.execs = execs;
    println!("トレード数: {}", self.execs.len());

    let min_pnl: f64 = self.execs.iter().fold(f64::NAN, |acc, exec| acc.min(exec.pnl));
    let max_pnl: f64 = self.execs.iter().fold(f64::NAN, |acc, exec| acc.max(exec.pnl));
    let mut chart = ChartBuilder::on(&area)
      .build_ranged(*SINCE..*UNTIL, min_pnl..max_pnl)
      .unwrap();
    let series = LineSeries::new(vec![(*SINCE, 0.0), (*UNTIL, 0.0)], &BLACK);
    chart.draw_series(series).unwrap();
    let series = LineSeries::new(self.execs.iter().map(|exec| (exec.timestamp, exec.pnl)), &BLUE);
    chart.draw_series(series).unwrap();

    let time_avg = self
      .execs
      .iter()
      .map(|exec| exec.timestamp.timestamp_millis() as f64)
      .sum::<f64>()
      / self.execs.len() as f64;
    let pnl_avg = self.execs.iter().map(|exec| exec.pnl).sum::<f64>() / self.execs.len() as f64;
    let time_variance = self
      .execs
      .iter()
      .map(|exec| (exec.timestamp.timestamp_millis() as f64 - time_avg).powi(2))
      .sum::<f64>()
      / self.execs.len() as f64;
    let covariance = self
      .execs
      .iter()
      .map(|exec| (exec.timestamp.timestamp_millis() as f64 - time_avg) * (exec.pnl - pnl_avg))
      .sum::<f64>()
      / self.execs.len() as f64;
    let slope = covariance / time_variance;
    let intercept = pnl_avg - slope * time_avg;
    let series = LineSeries::new(
      vec![
        (*SINCE, SINCE.timestamp_millis() as f64 * slope + intercept),
        (*UNTIL, UNTIL.timestamp_millis() as f64 * slope + intercept),
      ],
      &BLUE,
    );
    chart.draw_series(series).unwrap();

    let pnl_avg = self
      .execs
      .iter()
      .zip(self.execs.iter().skip(1))
      .map(|(prev_exec, next_exec)| next_exec.pnl - prev_exec.pnl)
      .sum::<f64>()
      / (self.execs.len() as f64 - 1.0);
    let pnl_variance = self
      .execs
      .iter()
      .zip(self.execs.iter().skip(1))
      .map(|(prev_exec, next_exec)| (next_exec.pnl - prev_exec.pnl - pnl_avg).powi(2))
      .sum::<f64>()
      / (self.execs.len() as f64 - 1.0);
    let pnl_stddev = pnl_variance.sqrt();
    let bet = max_position.max(-min_position);
    let return_avg = self.pnl / self.execs.len() as f64;
    println!("シャープレシオ: {}", return_avg / pnl_stddev);
    println!("利益: ${}", self.pnl);
    println!("利率: {:.0}%", self.pnl / bet / max_price * 100.0);

    let gurantee_trades = 9.0 * (pnl_stddev / return_avg).powi(2);
    println!("元本保証に必要なトレード数: {}", gurantee_trades);
    let gurantee_days = gurantee_trades as i32 / self.execs.len() as i32;
    println!("元本保証に必要な期間: {}日", gurantee_days);

    let mut profit = 0.0;
    let mut loss = 0.0;
    let mut prev_pnl = 0.0;
    let mut max_pnl = 0.0;
    let mut max_drawdown = 0.0;
    for exec in self.execs.iter() {
      let pnl_diff = exec.pnl - prev_pnl;
      if 0.0 < pnl_diff {
        profit += pnl_diff;
      } else if 0.0 > pnl_diff {
        loss -= pnl_diff;
      }
      prev_pnl = exec.pnl;

      if max_pnl < exec.pnl {
        max_pnl = exec.pnl;
      }

      let drawdown = max_pnl - exec.pnl;
      if max_drawdown < drawdown {
        max_drawdown = drawdown;
      }
    }
    println!("順利益: ${}", profit);
    println!("純損失: ${}", loss);
    println!("プロフィットファクター: {}", profit / loss);
    println!("最大ドローダウン: ${}", max_drawdown);
  }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Execution {
  timestamp: DateTime<Utc>,
  price: f64,
  pnl: f64,
  position: Position,
}

#[derive(Debug, Clone, PartialEq)]
struct LaggedOrder {
  since: DateTime<Utc>,
  until: DateTime<Utc>,
  order: Order,
}

#[derive(Debug, Clone, PartialEq)]
struct LaggedStop {
  since: DateTime<Utc>,
  until: DateTime<Utc>,
  stop: Stop,
}
