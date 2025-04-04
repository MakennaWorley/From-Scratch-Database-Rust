use crate::table::data::{Table};

impl Table {
    pub fn begin_transaction(&mut self) -> Result<(), String> {
        if self.transaction_backup.is_some() {
            return Err("Transaction already in progress".into());
        }
        self.transaction_backup = Some(self.rows.clone());
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<(), String> {
        if let Some(backup) = self.transaction_backup.take() {
            self.rows = backup;
            self.rebuild_all_indexes(); // restore consistency
            Ok(())
        } else {
            Err("No transaction to rollback".into())
        }
    }

    pub fn commit_transaction(&mut self) -> Result<(), String> {
        if self.transaction_backup.is_some() {
            self.transaction_backup = None;
            Ok(())
        } else {
            Err("No transaction to commit".into())
        }
    }
}