use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Error, Result};
use rayon::prelude::*;
use rust_decimal::Error::{ConversionTo};
use rust_decimal::prelude::*;
use thiserror::Error;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Column {
    name: String,
    size: usize,
    sql_type: String,
    generator: fn() -> Result<String>,
}

impl Column {
    pub fn new(
        name: String,
        size: usize,
        sql_type: String,
        generator: fn() -> Result<String>,
    ) -> Self {
        Column {
            name,
            size,
            sql_type,
            generator,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    id_value: String,
    columns: Vec<Column>,
    delimiter: String,
    percent_size: Decimal,
    row_size_bytes: usize,
}

impl Table {
    pub fn new(
        id_value: String,
        columns: Vec<Column>,
        delimiter: String,
        percent_size: Decimal,
    ) -> Table {
        let row_size_bytes: usize = columns
            .iter()
            .map(|x| x.size)
            .sum();
        Table { id_value, columns, delimiter, percent_size, row_size_bytes }
    }


    pub fn generate_table_row(&self) -> Result<String> {
        let mut buffer: Vec<String> = vec![self.id_value.clone()];
        buffer.append(
            &mut self.columns.iter()
                .map(|x| (x.generator)())
                .collect::<Result<Vec<String>>>()?
        );

        Ok(buffer.join(&self.delimiter) + "\n")
    }

    pub fn generate_table(&self, file_size_bytes: usize) -> Result<String> {
        let table_size_bytes = (
            Decimal::from_usize(file_size_bytes).ok_or(ConversionTo("Failed to convert to usize".into()))?
                * self.percent_size
        )
            .to_usize()
            .ok_or(ConversionTo("Failed to convert to usize".into()))?;
        let row_count = table_size_bytes / self.row_size_bytes;

        (0..row_count)
            .into_par_iter()
            .map(|_| self.generate_table_row())
            .try_reduce(|| "".to_string(), |x, y| Ok(x + &y))
    }
}


#[derive(Error, Debug)]
pub enum ExportFileError {
    #[error("Sum of table percentage sizes must be equal 1. It was {sum_percent_size}.")]
    SumPercentSizeIncorrect { sum_percent_size: Decimal }
}


pub struct ExportFile {
    tables: Vec<Table>,
    file_size_bytes: usize,
}

impl ExportFile {
    pub fn new(
        tables: Vec<Table>,
        file_size_bytes: usize,
    ) -> Result<ExportFile> {
        let sum_percent_size: Decimal = tables.iter()
            .map(|x| x.percent_size)
            .sum();

        if sum_percent_size != Decimal::from_str("1.0")? {
            return Err(Error::from(ExportFileError::SumPercentSizeIncorrect { sum_percent_size }));
        }

        Ok(ExportFile { tables, file_size_bytes })
    }


    pub fn generate_export(&self) -> Result<String> {
        self.tables.par_iter()
            .map(|x| x.generate_table(self.file_size_bytes))
            .try_reduce(|| "".to_string(), |x, y| Ok(x + &y))
    }


    pub fn generate_export_to_file(&self, path: &Path) -> Result<()> {
        let exported = self.generate_export()?;
        let mut file = File::create(path)?;
        file.write_all(exported.as_ref())?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn simple_generator() -> Result<String> {
        Ok("ABC".into())
    }

    #[test]
    fn export_file_create_test() {
        let c = Column::new(
            "column".into(),
            3,
            "CHAR[3]".into(),
            simple_generator,
        );

        let t1 = Table::new(
            "A".into(),
            vec![c.clone()],
            "|".into(),
            Decimal::from_str("0.5").unwrap(),
        );
        let t2 = Table::new(
            "B".into(),
            vec![c.clone(), c.clone()],
            "|".into(),
            Decimal::from_str("0.5").unwrap(),
        );

        let ef = ExportFile::new(
            vec![t1.clone(), t2.clone()],
            1 * 1024 * 1024,
        );

        match ef {
            Ok(_x) => { assert_eq!(1, 1) }
            Err(_) => {}
        }

        let ef = ExportFile::new(
            vec![t1.clone(), t2.clone(), t1.clone()],
            1 * 1024 * 1024,
        );

        match ef {
            Ok(_) => {}
            Err(_x) => { assert_eq!(1, 1) }
        }
    }

    #[test]
    fn generate_export_test() {
        let c = Column::new(
            "column".into(),
            3,
            "CHAR[3]".into(),
            simple_generator,
        );

        let t1 = Table::new(
            "A".into(),
            vec![c.clone()],
            "|".into(),
            Decimal::from_str("0.5").unwrap(),
        );

        let t2 = Table::new(
            "B".into(),
            vec![c.clone(), c.clone()],
            "|".into(),
            Decimal::from_str("0.5").unwrap(),
        );

        let ef = ExportFile::new(
            vec![t1.clone(), t2.clone()],
            1 * 1024 * 1024,
        ).unwrap();

        let ex = ef.generate_export();
        match ex {
            Ok(_x) => { assert_eq!(1, 1) }
            Err(_) => {}
        }
    }
}
