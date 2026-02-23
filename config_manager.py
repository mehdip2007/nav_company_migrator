import configparser
import os

class ConfigManager:
    def __init__(self, config_path="config.ini"):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def get_live_connection_params(self):
        return {
            "server": self.config["SQL_SERVER"]["server"],
            "port": self.config["SQL_SERVER"].get("port", "1433"),
            "database": self.config["SQL_SERVER"]["database"],
            "user": self.config["SQL_SERVER"]["username"],
            "password": self.config["SQL_SERVER"]["password"],
            "driver": self.config["SQL_SERVER"].get("driver", "ODBC Driver 17 for SQL Server"),
            "trust": self.config["SQL_SERVER"].get("TrustServerCertificate", "yes"),
        }

    def get_archive_database_name(self):
        return self.config["ARCHIVE_DB"]["database"]

    def get_company_name(self):
        return self.config["MIGRATION"].get("company_name", "").strip()

    def get_batch_size(self):
        return int(self.config["MIGRATION"].get("batch_size", "50000"))
