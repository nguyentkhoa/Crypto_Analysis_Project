# Crypto_Analysis_Project
# Crypto Data Warehouse (DWH) Design

This repository contains the **Data Warehouse design for granular 1-minute crypto data** with a **star schema** structure.

## 🚀 Overview

- Designed for **tracking price, volume, and trade data in 1-minute granularity**.
- Supports building **real-time dashboards**, anomaly detection, and ML pipelines for crypto analytics.
- Uses **two dimension tables**:
  - `Dim_DateTime`: Date, hour, minute, weekday.
  - `Dim_Coin`: Coin details.
- Uses **three fact tables**:
  - `Fact_Candle_1Min`: OHLCV data per minute.
  - `Fact_Ticker_1Min`: Ticker snapshot data per minute.
  - `Fact_Trade_1M`: Trade-level data per minute.

## 🗂️ ERD

![Crypto DWH ERD](./path_to_your_ERD_image.png)

## 🌟 Features

✅ Star schema for high-performance analytics.  
✅ Enables time-based and coin-based slicing and aggregation.  
✅ Supports dashboards, liquidity analysis, volatility heatmaps.  
✅ Ready for Snowflake, BigQuery, Postgres deployments.

## ⚡ Next Steps

- ETL pipelines from exchanges (Binance, Bitget).
- Power BI/Superset dashboards on top of the DWH.
- ML pipelines for price forecasting and anomaly alerts.
- Integration with your trading signals platform.

## 📜 License

MIT

## 🤝 Contributions

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
