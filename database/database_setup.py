import pandas
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger(__name__)

DATABASE_URL = "sqlite:///asset_tracking.db"

#establish engine and base
engine = create_engine(DATABASE_URL, echo=True)

base = declarative_base()

class Asset(base):
    __tablename__ = 'assets'
    ticker = Column(String, unique=True, nullable=False)
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    amount_held = Column(Float, nullable=False)
    total_value = Column(Float, nullable=False)
    last_value = Column(Float, nullable=False)
    last_updated = Column(String, nullable=False)
    
    def __repr__(self):
        return f"<Asset(ticker={self.ticker}, name={self.name}, amount_held={self.amount_held}, total_value={self.total_value}, last_value={self.last_value}, last_updated={self.last_updated})>"
    
    def init_db():
        logger.info("Creating database and tables if they do not exist...")
        base.metadata.create_all(engine)
        logger.info("Database setup complete.")
        
    def insert_assets_from_csv(csv_file: str):
        """_summary_
        converts the holdings csv into a database entry to track assets

        Args:
            csv_file (str): holdings CSV file path
        """
        try:
            asset_df = pandas.read_csv(csv_file)
            #clean
            asset_df.to_sql('assets', con=engine, if_exists='append', index=False)
            logger.info("Asset data inserted successfully.")
        except Exception:
            logger.exception("Error inserting asset data.")
        
