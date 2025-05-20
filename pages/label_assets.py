import os
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv


def render_label_assets_page():
    """
    子页面：批量和单个 Instrument 标签管理（同步到 instruments.labels 列），支持锁定功能
    """
    load_dotenv()
    DB_CFG = {
        'host': os.getenv('DB_HOST', '127.0.0.1'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('INSTR_DB', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()

    # 初始化表和列
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

    # session_state 初始化
    if 'locked' not in st.session_state:
        st.session_state.locked = set()
    if 'editing' not in st.session_state:
        st.session_state.editing = None

    # 操作回调
    def lock_inst(inst_id):
        st.session_state.locked.add(inst_id)
    def unlock_inst(inst_id):
        st.session_state.locked.discard(inst_id)
    def start_edit(inst_id):
        st.session_state.editing = inst_id if st.session_state.editing != inst_id else None

    def refresh_data():
        df = pd.read_sql('SELECT instrument_id, labels FROM instruments ORDER BY instrument_id', conn)
        df['labels'] = df['labels'].apply(lambda x: x if isinstance(x, list) else [])
        return df, dict(zip(df['instrument_id'], df['labels']))

    # 数据加载
    instr_df, labels_map = refresh_data()
    labels_df = pd.read_sql('SELECT id, name FROM labels ORDER BY name', conn)

    st.title("🔖 Label Assets 管理")

    # 新建标签
    st.subheader('新建标签')
    new_lbl = st.text_input('输入新标签名称', key='new_label')
    if st.button('创建新标签') and new_lbl:
        cur.execute('INSERT INTO labels(name) VALUES(%s) ON CONFLICT DO NOTHING;', (new_lbl,))
        conn.commit()
        labels_df = pd.read_sql('SELECT id, name FROM labels ORDER BY name', conn)
        st.success(f"标签 '{new_lbl}' 已创建。")

    # 批量标签分配
    st.subheader('批量标签分配 (锁定的将被排除)')
    batch_label = st.selectbox('1) 选择要分配的标签', labels_df['name'].tolist())
    candidates = [inst for inst, arr in labels_map.items() if batch_label not in arr and inst not in st.session_state.locked]
    sel_batch = st.multiselect('2) 选择 Instruments', candidates)
    if st.button('应用批量标签'):
        if not sel_batch:
            st.warning('请先选择至少一个 Instrument')
        else:
            lbl_id_raw = labels_df.loc[labels_df['name'] == batch_label, 'id'].iat[0]
            lbl_id = int(lbl_id_raw)
            for inst in sel_batch:
                cur.execute(
                    'INSERT INTO instrument_labels(instrument_id,label_id) VALUES(%s,%s) ON CONFLICT DO NOTHING;', 
                    (inst, lbl_id)
                )
                cur.execute(
                    'UPDATE instruments SET labels = array_append(COALESCE(labels,ARRAY[]::TEXT[]), %s) WHERE instrument_id=%s;', 
                    (batch_label, inst)
                )
            conn.commit()
            instr_df, labels_map = refresh_data()
            st.success(f"已为 {len(sel_batch)} 个 Instruments 批量添加 '{batch_label}' 标签。")

    # 列表与锁定/编辑
    st.subheader('Instrument 列表与锁定')
    cols = st.columns((4, 2, 2, 2))
    cols[0].write('Instrument')
    cols[1].write('Labels')
    cols[2].write('Lock')
    cols[3].write('操作')
    for inst, arr in labels_map.items():
        c0, c1, c2, c3 = st.columns((4, 2, 2, 2))
        c0.write(inst)
        c1.write(','.join(arr))
        # 锁定/解锁按钮
        if inst in st.session_state.locked:
            if c2.button('🟢 解锁', key=f'unlock_{inst}', on_click=unlock_inst, args=(inst,)):
                st.success(f"已解锁 {inst}")
        else:
            if c2.button('🔴 锁定', key=f'lock_{inst}', on_click=lock_inst, args=(inst,)):
                st.success(f"已锁定 {inst}")
        # 编辑按钮
        if c3.button('✏️ 编辑', key=f'edit_{inst}', on_click=start_edit, args=(inst,)):
            pass
        # 行内编辑区
        if st.session_state.editing == inst:
            with st.expander(f"编辑 {inst}", expanded=True):
                current = labels_map.get(inst, [])
                new_sel = st.multiselect(
                    '编辑标签（勾选即保留，否则删除）', 
                    labels_df['name'].tolist(), 
                    default=current, 
                    key=f'sel_{inst}'
                )
                if st.button('保存', key=f'save_{inst}'):
                    cur.execute('DELETE FROM instrument_labels WHERE instrument_id=%s;', (inst,))
                    for lbl in new_sel:
                        lid_raw = labels_df.loc[labels_df['name'] == lbl, 'id'].iat[0]
                        lid = int(lid_raw)
                        cur.execute(
                            'INSERT INTO instrument_labels(instrument_id,label_id) VALUES(%s,%s) ON CONFLICT DO NOTHING;', 
                            (inst, lid)
                        )
                    cur.execute('UPDATE instruments SET labels=%s WHERE instrument_id=%s;', (new_sel, inst))
                    conn.commit()
                    instr_df, labels_map = refresh_data()
                    st.success(f"Instrument '{inst}' 的标签已更新：{new_sel}")
                    st.session_state.editing = None

    cur.close()
    conn.close()
