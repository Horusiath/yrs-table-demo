use csv::Reader;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::sync::Arc;
use yrs::encoding::serde::from_any;
use yrs::types::array::ArrayIter;
use yrs::types::ToJson;
use yrs::{Any, Array, ArrayRef, Map, MapPrelim, MapRef, Out, ReadTxn, TransactionMut};

pub struct Table {
    cols: ArrayRef,
    rows: ArrayRef,
    cells: MapRef,
}

impl Table {
    pub fn new(root: MapRef, txn: &mut TransactionMut) -> Self {
        let cols: ArrayRef = root.get_or_init(txn, "cols");
        let rows: ArrayRef = root.get_or_init(txn, "rows");
        let cells: MapRef = root.get_or_init(txn, "cells");
        Self { cols, rows, cells }
    }

    pub fn row_count<T: ReadTxn>(&self, txn: &T) -> u32 {
        self.rows.len(txn)
    }

    pub fn col_count<T: ReadTxn>(&self, txn: &T) -> u32 {
        self.cols.len(txn)
    }

    pub fn import(&self, txn: &mut TransactionMut, reader: &mut Reader<File>) -> u32 {
        let headers = reader.headers().unwrap();
        let headers: Vec<_> = headers.iter().map(|s| s.to_string()).collect();
        let mut column_ids = Vec::with_capacity(headers.len());
        let mut column_id_index = HashSet::with_capacity(headers.len());
        for header in headers {
            let mut col_id = 0;
            {
                // generate next random unique column id
                while {
                    col_id = fastrand::u32(..);
                    !column_id_index.insert(col_id)
                } {}
            };
            column_ids.push(col_id);
            let len = self.cols.len(txn);
            self.cols.insert(
                txn,
                len,
                MapPrelim::from([
                    ("id", Any::BigInt(col_id as i64)),
                    ("name", Any::from(header)),
                    ("width", Any::from(130)),
                ]),
            );
        }
        drop(column_id_index);

        let rows: Vec<_> = reader
            .records()
            .map(|record| {
                let record = record.unwrap();
                let record: Vec<_> = record.iter().map(Self::parse).collect();
                record
            })
            .collect();

        let mut row_ids = Vec::with_capacity(rows.len());
        let mut row_id_index = HashSet::with_capacity(rows.len());
        for _ in 0..rows.len() {
            let mut row_id = 0;
            {
                // generate next random unique row id
                while {
                    row_id = fastrand::u32(..);
                    !row_id_index.insert(row_id)
                } {}
            };
            row_ids.push(row_id);
            self.rows.insert(
                txn,
                0,
                MapPrelim::from([("id", Any::from(row_id)), ("height", Any::from(30))]),
            );
        }

        let mut cell_count = 0;
        for (row_idx, record) in rows.into_iter().enumerate() {
            for (col_idx, cell) in record.into_iter().enumerate() {
                let cell_id = format!("{:x}:{:x}", row_ids[row_idx], column_ids[col_idx]);
                self.cells.insert(txn, cell_id, cell);
                cell_count += 1;
            }
        }
        cell_count
    }

    fn parse(input: &str) -> Any {
        if let Ok(n) = input.parse::<i64>() {
            Any::BigInt(n)
        } else if let Ok(n) = input.parse::<f64>() {
            Any::Number(n)
        } else {
            Any::from(input)
        }
    }

    pub fn columns<T: ReadTxn>(&self, tx: &T) -> Vec<Column> {
        let any = self.cols.to_json(tx);
        from_any(&any).unwrap()
    }

    pub fn rows<'tx, T: ReadTxn>(&self, tx: &'tx T) -> Rows<'tx, T> {
        let columns = self.columns(tx);
        let iter = self.rows.iter(tx);
        Rows {
            row_iter: iter,
            tx,
            columns: columns.into(),
            cells: self.cells.clone(),
        }
    }
}

#[derive(Deserialize)]
pub struct Column {
    pub id: i64,
    pub name: String,
    pub width: u32,
}

#[derive(Deserialize)]
pub struct RowInfo {
    pub id: i64,
    pub height: u32,
}

pub struct Rows<'tx, T: ReadTxn> {
    row_iter: ArrayIter<&'tx T, T>,
    tx: &'tx T,
    columns: Arc<[Column]>,
    cells: MapRef,
}

impl<'tx, T> Iterator for Rows<'tx, T>
where
    T: ReadTxn,
{
    type Item = Row<'tx, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let out = self.row_iter.next()?;
        let row_info: RowInfo = from_any(&out.to_json(self.tx)).ok()?;
        Some(Row {
            row_info,
            columns: self.columns.clone(),
            cells: self.cells.clone(),
            tx: self.tx,
        })
    }
}

pub struct Row<'tx, T> {
    pub row_info: RowInfo,
    columns: Arc<[Column]>,
    cells: MapRef,
    tx: &'tx T,
}

impl<'tx, T> Row<'tx, T>
where
    T: ReadTxn,
{
    /// Returns the raw cell data for this row.
    pub fn raw(&self) -> Vec<Out> {
        let mut data = Vec::with_capacity(self.columns.len());
        for col in self.columns.iter() {
            let cell_id = format!("{:x}:{:x}", self.row_info.id, col.id);
            let cell = self.cells.get(self.tx, &cell_id).unwrap();
            data.push(cell.clone());
        }
        data
    }
}
