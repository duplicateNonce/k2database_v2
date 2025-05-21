import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# 缓存数据库连接引擎
@st.cache_resource
def get_engine():
    host     = os.getenv('DB_HOST', '127.0.0.1')
    port     = os.getenv('DB_PORT', '5432')
    dbname   = os.getenv('INSTR_DB', 'postgres')
    user     = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', '')
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)

def render_price_change_page():
    st.header("涨跌幅排行榜（实时更新）")
    engine = get_engine()

    # 定义各时间窗口
    time_windows = [
        ('5m',  '5 分钟'),
        ('15m', '15 分钟'),
        ('30m', '30 分钟'),
        ('1h',  '1 小时'),
        ('4h',  '4 小时'),
        ('12h', '12 小时'),
        ('24h', '24 小时'),
    ]

    tabs = st.tabs([label for _, label in time_windows])
    for (win, label), tab in zip(time_windows, tabs):
        with tab:
            # —— Top 10 单币种 —— 
            st.subheader(f"{label} 币对 Top 10")
            top10_sql = f"""
                SELECT 
                  c.symbol                 AS instrument_id,
                  c.price_change_percent_{win} AS pct
                FROM coinmarket_aggregated c
                ORDER BY pct DESC
                LIMIT 10;
            """
            df_top10 = pd.read_sql_query(top10_sql, engine)
            # 安全格式化：如果 pct 为 null/NaN，就显示 N/A
            df_top10['pct_display'] = df_top10['pct'].apply(
                lambda x: f"{x:.2%}" if pd.notnull(x) else "N/A"
            )
            st.table(
                df_top10
                  .rename(columns={'instrument_id':'币对','pct_display':'涨跌幅'})
                  [['币对','涨跌幅']]
            )

            # —— Top 5 标签组 —— 
            st.markdown("**标签组 Top 5（按平均涨幅）**")
            label_sql = f"""
                WITH exploded AS (
                  SELECT
                    unnest(i.labels)                     AS label,
                    c.price_change_percent_{win}         AS pct,
                    c.symbol                             AS instrument_id
                  FROM instruments i
                  JOIN coinmarket_aggregated c
                    ON i.instrument_id = c.symbol
                )
                SELECT
                  label,
                  AVG(pct)                                 AS avg_pct,
                  PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY pct)
                                                          AS median_pct,
                  MAX(pct)                                 AS max_pct,
                  MIN(pct)                                 AS min_pct,
                  ARRAY_AGG(instrument_id)                 AS instruments
                FROM exploded
                GROUP BY label
                ORDER BY avg_pct DESC
                LIMIT 5;
            """
            df_label = pd.read_sql_query(label_sql, engine)
            # 安全格式化各统计值
            for stat in ['avg_pct','median_pct','max_pct','min_pct']:
                df_label[f"{stat}_disp"] = df_label[stat].apply(
                    lambda x: f"{x:.2%}" if pd.notnull(x) else "N/A"
                )

            # 用 expander 展开每个标签详情
            for _, row in df_label.iterrows():
                with st.expander(f"标签：{row['label']}"):
                    st.write({
                        '平均涨跌幅':    row['avg_pct_disp'],
                        '中位数涨跌幅':  row['median_pct_disp'],
                        '最高涨跌幅':    row['max_pct_disp'],
                        '最低涨跌幅':    row['min_pct_disp'],
                    })
                    st.write("包含币对：", row['instruments'])
