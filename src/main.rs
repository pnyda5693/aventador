mod exchange;
mod models;
mod prelude;
mod strategy;

use chrono::Duration;
use futures::future::Either;
use futures::prelude::*;
use lazy_static::lazy_static;
use prelude::*;
use std::env;
use tokio::pin;
use tokio::time::interval;

lazy_static! {
  static ref PING_INTERVAL: Duration = Duration::seconds(15);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
  setup_log();

  let mut exchange = connect_exchange().await;
  let mut strategy = Strategy::new(&exchange).await;
  let mut interval = interval(PING_INTERVAL.to_std().unwrap());

  loop {
    let timer = interval.tick();
    pin!(timer);
    let incoming = exchange.next();
    pin!(incoming);

    match future::select(timer, incoming).await {
      Either::Left(_) => {
        // Send ping
        exchange.send(()).await.unwrap();
      }
      Either::Right((None, _)) => {
        // systemdに再起動させる
        panic!("WebSocket connection closed")
      }
      Either::Right((Some(Incoming::Trade(trades)), _)) => {
        for trade in trades {
          strategy.handle_trade(&exchange, trade).await
        }
      }
      Either::Right((Some(Incoming::Order(orders)), _)) => {
        for order in orders {
          strategy.handle_order(&exchange, order).await
        }
      }
      Either::Right((Some(Incoming::Position(positions)), _)) => {
        for position in positions {
          // 正常終了の場合は再起動されない
          if position.is_liquidated {
            return;
          }
          strategy.handle_position(&exchange, position).await
        }
      }
    }
  }
}

fn setup_log() {
  match std::env::var("RUST_LOG") {
    Ok(_) => env_logger::init(),
    Err(_) => env_logger::builder()
      .filter_module("aventador", log::LevelFilter::Trace)
      .init(),
  }
}

#[cfg(debug_assertions)]
async fn connect_exchange() -> Box<dyn Exchange> {
  log::info!("Running backtests...");
  Box::new(Backtest::new())
}

#[cfg(not(debug_assertions))]
async fn connect_exchange() -> Box<dyn Exchange> {
  log::warn!("Running on mainnet...");
  let exchange = env::var("AVENTADOR_MARKET").unwrap();
  let mut ftx = Ftx::new(exchange).await;
  ftx.subscribe(Topic::Position).await;
  ftx.subscribe(Topic::Trade).await;
  Box::new(ftx)
}
