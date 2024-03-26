use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Write};
use std::path::{Path, PathBuf};

use anyhow::{Error, Result};
use rayon::prelude::*;
use rust_decimal::Error::ConversionTo;
use rust_decimal::prelude::*;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    size: u64,
    sql_type: String,
    generator: fn() -> Result<String>,
}

impl Column {
    pub fn new(
        name: String,
        size: u64,
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
    row_size_bytes: u64,
}

impl Table {
    pub fn new(
        id_value: String,
        columns: Vec<Column>,
        delimiter: String,
        percent_size: Decimal,
    ) -> Table {
        let row_size_bytes: u64 = columns
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

    pub fn generate_table_row_vec(&self) -> Result<Vec<String>> {
        let mut buffer: Vec<String> = vec![self.id_value.clone()];
        buffer.append(
            &mut self.columns.iter()
                .map(|x| (x.generator)())
                .collect::<Result<Vec<String>>>()?
        );

        Ok(buffer)
    }

    pub fn generate_table(&self, file_size_bytes: u64) -> Result<String> {
        let table_size_bytes = (
            Decimal::from(file_size_bytes)
                * self.percent_size
        )
            .to_u64()
            .ok_or(ConversionTo("Failed to convert to u64".into()))?;
        let row_count = table_size_bytes / self.row_size_bytes;

        (0..row_count)
            .into_par_iter()
            .map(|_| self.generate_table_row())
            .try_reduce(|| "".to_string(), |x, y| Ok(x + &y))
    }

    pub fn generate_table_vec(&self, file_size_bytes: u64) -> Result<Vec<Vec<String>>> {
        let table_size_bytes = (
            Decimal::from(file_size_bytes)
                * self.percent_size
        )
            .to_u64()
            .ok_or(ConversionTo("Failed to convert to u64".into()))?;
        let row_count = table_size_bytes / self.row_size_bytes;

        (0..row_count)
            .into_par_iter()
            .map(|_| self.generate_table_row_vec())
            .collect()
    }
}


#[derive(Error, Debug)]
pub enum ExportFileError {
    #[error("Sum of table percentage sizes must be equal 1. It was {sum_percent_size}.")]
    SumPercentSizeIncorrect { sum_percent_size: Decimal },
    #[error("Table {table} has a duplicate column {column}.")]
    DuplicateColumns { table: String, column: String },
    #[error("Export File contains duplicate table {table}.")]
    DuplicateTables { table: String },
    #[error("Too many files to generate {files}")]
    TooManyFiles { files: u64 },
    #[error("ReduceFailed")]
    ReduceFailed,
}


pub struct ExportFile {
    tables: Vec<Table>,
    number_of_files: u64,
    file_size_bytes: u64,
}

impl ExportFile {
    pub fn new(
        tables: Vec<Table>,
        data_size_bytes: u64,
        number_of_files: u64,
    ) -> Result<ExportFile> {
        if number_of_files >= data_size_bytes {
            return Err(Error::from(ExportFileError::TooManyFiles { files: number_of_files }));
        }

        let file_size_bytes = data_size_bytes / number_of_files;

        let is_possible = tables.iter()
            .map(|x| Decimal::from(file_size_bytes) * x.percent_size >= Decimal::from(x.row_size_bytes))
            .reduce(|x, y| x && y)
            .ok_or(Error::from(ExportFileError::ReduceFailed))?;

        if !is_possible {
            return Err(Error::from(ExportFileError::TooManyFiles { files: number_of_files }));
        }

        let sum_percent_size: Decimal = tables.iter()
            .map(|x| x.percent_size)
            .sum();

        if sum_percent_size != Decimal::from_str("1.0")? {
            return Err(Error::from(ExportFileError::SumPercentSizeIncorrect { sum_percent_size }));
        }

        Ok(ExportFile { tables, number_of_files, file_size_bytes })
    }


    pub fn generate_export(&self) -> Result<String> {
        self.tables.par_iter()
            .map(|x| x.generate_table(self.file_size_bytes))
            .try_reduce(|| "".to_string(), |x, y| Ok(x + &y))
    }


    pub fn generate_raw_tables(&self) -> HashMap<String, Result<Vec<Vec<String>>>> {
        self.tables.par_iter()
            .map(|x| {
                let mut m: HashMap<String, Result<Vec<Vec<String>>>> = HashMap::new();
                m.insert(
                    x.id_value.clone(),
                    x.generate_table_vec(self.file_size_bytes),
                );
                m
            })
            .reduce(|| HashMap::new(), |a, b| {
                a.into_iter().chain(b).collect()
            })
    }


    pub fn raw_tables_to_string(tables: HashMap<String, Result<Vec<Vec<String>>>>, delimiter: &str) -> Result<String> {
        Ok(tables.par_iter()
            .map(|x|
                {
                    x.1.as_ref()
                        .par_iter()
                        .map(|y| y.par_iter()
                            .map(|z| z.join(delimiter))
                            .reduce(|| "".to_string(), |a, b| a + &b)
                        )
                        .reduce(|| "".to_string(), |a, b| a + &b)
                })
            .reduce(|| "".to_string(), |a, b| {
                a + &b
            }))
    }


    pub fn generate_export_to_file(&self, path: &Path) -> Result<()> {
        let exported = self.generate_export()?;
        let mut file = File::create(path)?;
        file.write_all(exported.as_ref())?;
        Ok(())
    }


    pub fn generate_all_files(&self, folder_path: &Path) -> Result<()> {
        fs::create_dir_all(folder_path)?;

        (0..self.number_of_files.to_owned()).into_par_iter()
            .try_for_each(|x| -> Result<()> {
                let file_path = PathBuf::new()
                    .join(folder_path)
                    .join(format!(
                        "file_{}_{}_{}.txt",
                        &self.file_size_bytes,
                        &self.number_of_files,
                        &x
                    ));

                self.generate_export_to_file(file_path.as_path())?;

                Ok(())
            })?;

        Ok(())
    }


    pub fn build_schema(&self) -> Result<HashMap<String, HashMap<String, String>>> {
        let mut schema: HashMap<String, HashMap<String, String>> = HashMap::new();

        for table in self.tables.as_slice() {
            let mut columns: HashMap<String, String> = HashMap::new();

            for column in table.columns.as_slice() {
                if columns.contains_key(&column.name) {
                    return Err(Error::from(ExportFileError::DuplicateColumns {
                        table: table.id_value.clone(),
                        column: column.name.clone(),
                    }));
                }
                columns.insert(column.name.clone(), column.sql_type.clone());
            }

            if schema.contains_key(&table.id_value) {
                return Err(Error::from(ExportFileError::DuplicateTables {
                    table: table.id_value.clone()
                }));
            }

            schema.insert(table.id_value.clone(), columns);
        }

        Ok(schema)
    }


    pub fn get_schema_json_str(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.build_schema()?)?)
    }


    pub fn schema_json(
        &self,
        path: &Path,
    ) -> Result<()> {
        let schema = self.get_schema_json_str()?;
        let mut file = File::create(path)?;
        file.write_all(schema.as_ref())?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use std::result::Result::Ok;
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
            1,
        );

        match ef {
            Ok(_x) => { assert_eq!(1, 1) }
            Err(_) => {}
        }

        let ef = ExportFile::new(
            vec![t1.clone(), t2.clone(), t1.clone()],
            1 * 1024 * 1024,
            1,
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
            1,
        ).unwrap();

        let ex = ef.generate_export();
        match ex {
            Ok(_x) => { assert_eq!(1, 1) }
            Err(_) => {}
        }
    }


    #[test]
    fn get_schema_json_str_test() {
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
            Decimal::from_str("1.0").unwrap(),
        );

        let ef = ExportFile::new(
            vec![t1.clone()],
            1 * 1024 * 1024,
            1,
        ).unwrap();

        let schema = ef.get_schema_json_str();

        match schema {
            Ok(x) => {
                assert_eq!(x, r#"{"A":{"column":"CHAR[3]"}}"#);
            }
            Err(_) => {}
        }
    }
}
