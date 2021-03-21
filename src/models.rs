use fxdb::*;
use std::hash::{Hash, Hasher};

pub(crate) static LEVERAGE: f64 = 10.0;
static MAINTENANCE_MARGIN: f64 = 0.5 * 0.01;

#[derive(Debug, Clone)]
pub(crate) struct Order {
  pub(crate) id: String,
  pub(crate) price: Option<f64>,
  pub(crate) amount: f64,
  pub(crate) filled: f64,
}

impl Hash for Order {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.id.hash(state)
  }
}

impl PartialEq for Order {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for Order {}

impl Order {
  pub(crate) fn is_buy(&self) -> bool {
    0.0 < self.amount
  }

  pub(crate) fn is_sell(&self) -> bool {
    0.0 > self.amount
  }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Stop {
  pub(crate) id: String,
  pub(crate) trigger: f64,
  pub(crate) price: Option<f64>,
  pub(crate) amount: f64,
}

impl Stop {
  pub(crate) fn is_buy(&self) -> bool {
    0.0 < self.amount
  }

  pub(crate) fn is_sell(&self) -> bool {
    0.0 > self.amount
  }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Position {
  pub(crate) price: f64,
  pub(crate) amount: f64,
  pub(crate) leverage: f64,
  pub(crate) is_liquidated: bool,
}

impl Position {
  pub(crate) fn neutral() -> Self {
    Position {
      price: 0.0,
      amount: 0.0,
      leverage: LEVERAGE,
      is_liquidated: false,
    }
  }

  pub(crate) fn is_buy(&self) -> bool {
    0.0 < self.amount
  }

  pub(crate) fn is_sell(&self) -> bool {
    0.0 > self.amount
  }

  pub(crate) fn exists(&self) -> bool {
    self.amount != 0.0
  }

  pub(crate) fn liquidation(&self) -> f64 {
    if self.is_buy() {
      self.price * self.leverage / (self.leverage + 1.0 - self.leverage * MAINTENANCE_MARGIN)
    } else if self.is_sell() {
      self.price * self.leverage / (self.leverage - 1.0 + self.leverage * MAINTENANCE_MARGIN)
    } else {
      panic!()
    }
  }
}

#[derive(Debug, Clone)]
pub(crate) enum Incoming {
  Trade(Vec<Trade>),
  Order(Vec<Order>),
  Position(Vec<Position>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Topic {
  Trade,
  Order,
  Position,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Error {
  AlreadyCancelled,
  InvalidState,
  InsufficientBalance,
  Unknown,
}
