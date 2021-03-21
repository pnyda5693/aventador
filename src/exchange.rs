use crate::prelude::*;
use async_trait::async_trait;
use chrono::prelude::*;
use futures::prelude::*;
use fxdb::*;
use tokio_tungstenite::tungstenite;

pub(crate) mod backtest;
pub(crate) use backtest::*;

pub(crate) mod ftx;
pub(crate) use ftx::*;

#[async_trait(?Send)]
pub(crate) trait Exchange: Unpin + Stream<Item = Incoming> + Sink<(), Error = tungstenite::Error> {
  async fn limit_order(&self, price: f64, amount: f64) -> Result<String, Error>;
  async fn market_order(&self, amount: f64) -> Result<String, Error>;
  async fn cancel_order(&self, order_id: &str) -> Result<(), Error>;
  async fn cancel_orders(&self) -> Result<(), Error>;
  async fn stop_market(&self, trigger: f64, amount: f64) -> Result<String, Error>;
  async fn stop_limit(&self, trigger: f64, price: f64, amount: f64) -> Result<String, Error>;
  async fn cancel_stop(&self, stop_id: &str) -> Result<(), Error>;
  async fn cancel_stops(&self) -> Result<(), Error>;
  async fn orders(&self) -> Result<Vec<Order>, Error>;
  async fn stops(&self) -> Result<Vec<Stop>, Error>;
  async fn trades(&self, since: DateTime<Utc>) -> Result<Vec<Trade>, Error>;
  async fn position(&self) -> Result<Position, Error>;
  async fn balance(&self) -> Result<f64, Error>;
  async fn leverage(&self) -> Result<f64, Error>;
}

#[async_trait(?Send)]
pub(crate) trait ExchangeExt {
  async fn close_position(&self) -> Result<(), Error>;
}

#[async_trait(?Send)]
impl<T: Exchange> ExchangeExt for T {
  async fn close_position(&self) -> Result<(), Error> {
    let position = self.position().await?;
    if position.exists() {
      self.market_order(-position.amount).await?;
    }

    Ok(())
  }
}
