# 文件：pages/label_assets.py

import os
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from config import secret_get

def render_label_assets_page():
    """
    子页面：批量和单个 Instrument 标签管理（同步到 instruments.labels 列），支持锁定功能。
    现在改为：从 instrument_labels + labels 两表联动，确保每个 instrument_id
    能正确归入所有已打的标签组里。
    """
    load_dotenv()
    DB_CFG = {
        'host':     secret_get('DB_HOST', '127.0.0.1'),
        'port':     secret_get('DB_PORT', '5432'),
        'dbname':   secret_get('INSTR_DB', 'postgres'),
        'user':     secret_get('DB_USER', 'postgres'),
        'password': secret_get('DB_PASSWORD', ''),
    }
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()

    # —— 1. 确保三张表结构存在 ——
    cur.execute('''
        CREATE TABLE IF NOT EXISTS labels(
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS instrument_labels(
            instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
            label_id      INTEGER NOT NULL REFERENCES labels(id),
            PRIMARY KEY(instrument_id,label_id)
        );
    ''')
    cur.execute('''
        ALTER TABLE instruments
        ADD COLUMN IF NOT EXISTS labels TEXT[];
    ''')
    conn.commit()

    # —— 2. Session 状态 —— 
    if 'locked' not in st.session_state:
        st.session_state.locked = set()
    if 'editing' not in st.session_state:
        st.session_state.editing = None

    def lock_inst(inst_id):
        st.session_state.locked.add(inst_id)
    def unlock_inst(inst_id):
        st.session_state.locked.discard(inst_id)
    def start_edit(inst_id):
        st.session_state.editing = inst_id if st.session_state.editing != inst_id else None

    # —— 3. 数据加载：改为从 instrument_labels + labels 拉 mapping —— 
    def refresh_data():
        # 3.1 从 instruments 读出所有 instrument_id 及 locked
        df_base = pd.read_sql(
            "SELECT instrument_id, locked FROM instruments ORDER BY instrument_id",
            conn
        )
        # 3.2 从映射表联 labels 表拼出 TEXT[] 列
        df_map = pd.read_sql("""
            SELECT i.instrument_id,
                   COALESCE(array_agg(l.name) FILTER (WHERE l.name IS NOT NULL), ARRAY[]::TEXT[]) AS labels
            FROM instruments i
            LEFT JOIN instrument_labels il ON i.instrument_id = il.instrument_id
            LEFT JOIN labels l            ON il.label_id      = l.id
            GROUP BY i.instrument_id
            ORDER BY i.instrument_id
        """, conn)
        # 3.3 合并 locked 信息
        df = pd.merge(df_base, df_map, on='instrument_id', how='left')
        # 确保 labels 列总是 Python list
        df['labels'] = df['labels'].apply(lambda x: x if isinstance(x, list) else [])
        # 构造一个字典：inst -> [labels]
        labels_map = dict(zip(df['instrument_id'], df['labels']))
        return df, labels_map

    instr_df, labels_map = refresh_data()
    labels_df = pd.read_sql('SELECT id, name FROM labels ORDER BY name', conn)

    st.title("🔖 编辑标的标签")

    # —— 4. 新建标签 ——
    st.subheader('🚩 新建标签')
    new_lbl = st.text_input('输入新标签名称', key='new_label')
    if st.button('创建新标签') and new_lbl:
        cur.execute(
            'INSERT INTO labels(name) VALUES(%s) ON CONFLICT DO NOTHING;',
            (new_lbl.strip(),)
        )
        conn.commit()
        labels_df = pd.read_sql('SELECT id, name FROM labels ORDER BY name', conn)
        st.success(f"标签 '{new_lbl}' 已创建。")

    # —— 5. 批量标签分配 —— 
    st.subheader('🎯 批量标签分配 (锁定的 Instruments 会被排除)')
    batch_label = st.selectbox('1) 选择要分配的标签', [''] + labels_df['name'].tolist())
    if batch_label:
        # 候选列表：没打过且未锁定
        candidates = [
            inst for inst, arr in labels_map.items()
            if batch_label not in arr and inst not in st.session_state.locked
        ]
        sel_batch = st.multiselect('2) 选择 Instruments', candidates)
        if st.button('应用批量标签'):
            if not sel_batch:
                st.warning('请至少选择一个 Instrument')
            else:
                lbl_id = int(labels_df.loc[labels_df['name']==batch_label, 'id'].iat[0])
                for inst in sel_batch:
                    # a) 插入中间表
                    cur.execute(
                        'INSERT INTO instrument_labels(instrument_id,label_id) '
                        'VALUES(%s,%s) ON CONFLICT DO NOTHING;',
                        (inst, lbl_id)
                    )
                    # b) 更新 instruments.labels 数组
                    cur.execute(
                        'UPDATE instruments '
                        'SET labels = array_append(COALESCE(labels,ARRAY[]::TEXT[]), %s) '
                        'WHERE instrument_id=%s;',
                        (batch_label, inst)
                    )
                conn.commit()
                instr_df, labels_map = refresh_data()
                st.success(f"已为 {len(sel_batch)} 个 Instruments 批量添加标签：{batch_label}")

    # —— 6. Instrument 列表 & 锁定 / 单件编辑 —— 
    st.subheader('📋 Instruments 列表 与 操作')
    cols = st.columns((4, 2, 2, 2))
    cols[0].write('Instrument')
    cols[1].write('Labels')
    cols[2].write('锁定')
    cols[3].write('操作')
    for _, row in instr_df.iterrows():
        inst = row['instrument_id']
        arr  = row['labels']
        locked = bool(row['locked'])
        c0, c1, c2, c3 = st.columns((4, 2, 2, 2))
        c0.write(inst)
        c1.write(','.join(arr))
        # 锁定 / 解锁
        if locked:
            if c2.button('🟢 解锁', key=f'unlock_{inst}', on_click=unlock_inst, args=(inst,)):
                st.success(f"已解锁 {inst}")
        else:
            if c2.button('🔒 锁定', key=f'lock_{inst}', on_click=lock_inst, args=(inst,)):
                st.success(f"已锁定 {inst}")
        # 单件编辑
        if c3.button('✏️ 编辑', key=f'edit_{inst}', on_click=start_edit, args=(inst,)):
            pass
        # 展开编辑表单
        if st.session_state.editing == inst:
            with st.expander(f"编辑 {inst}", expanded=True):
                current = labels_map.get(inst, [])
                new_sel = st.multiselect(
                    '勾选保留标签(取消即删除)',
                    options=labels_df['name'].tolist(),
                    default=current,
                    key=f'sel_{inst}'
                )
                if st.button('保存', key=f'save_{inst}'):
                    # 先删旧映射
                    cur.execute('DELETE FROM instrument_labels WHERE instrument_id=%s;', (inst,))
                    # 再插入新映射
                    for lbl in new_sel:
                        lid = int(labels_df.loc[labels_df['name']==lbl, 'id'].iat[0])
                        cur.execute(
                            'INSERT INTO instrument_labels(instrument_id,label_id) '
                            'VALUES(%s,%s) ON CONFLICT DO NOTHING;',
                            (inst, lid)
                        )
                    # 同步 instruments.labels
                    cur.execute(
                        'UPDATE instruments SET labels=%s WHERE instrument_id=%s;',
                        (new_sel, inst)
                    )
                    conn.commit()
                    instr_df, labels_map = refresh_data()
                    st.success(f"'{inst}' 的标签已更新：{new_sel}")
                    st.session_state.editing = None

    cur.close()
    conn.close()
