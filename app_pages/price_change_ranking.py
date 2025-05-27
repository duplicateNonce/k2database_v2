import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

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
    """
    在 Streamlit 主应用中调用此函数，按 instrument label 列出所有资产
    """
    st.header("按标签列出所有资产")
    engine = get_engine()

    # 查询所有标签
    labels_sql = "SELECT id, name FROM labels ORDER BY name;"
    df_labels = pd.read_sql_query(labels_sql, engine)

    # 对每个标签分别列出资产
    for _, row in df_labels.iterrows():
        label_id = row['id']
        label_name = row['name']

        instr_sql = f"""
            SELECT il.instrument_id
            FROM instrument_labels il
            WHERE il.label_id = {label_id}
            ORDER BY il.instrument_id;
        """
        df_instr = pd.read_sql_query(instr_sql, engine)

        # 如果标签下没有资产，则跳过
        if df_instr.empty:
            continue

        # 展开显示该标签下的所有资产
        with st.expander(f"标签：{label_name} （共 {len(df_instr)} 个资产）", expanded=False):
            st.table(
                df_instr.rename(columns={'instrument_id': '资产符号'})
            )
