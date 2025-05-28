import streamlit as st
from app_pages.overview import render_overview
from app_pages.ohlcv import render_ohlcv_page
# from app_pages.strong_assets import render_strong_assets_page
# from app_pages.bottom_lift import render_bottom_lift_page
from app_pages.label_assets import render_label_assets_page
# from app_pages.price_change_by_label import render_price_change_by_label
from app_pages.combined_analysis import render_combined_page
from app_pages.watchlist import render_watchlist_page

# 使用 codex 分支中新加的登录凭证和 rerun 工具
from config import USER_CREDENTIALS
from utils import safe_rerun
import json
from uuid import uuid4
from pathlib import Path
import hashlib

PAGES = {
    "Overview": render_overview,
    "OHLCV": render_ohlcv_page,
    "综合分析": render_combined_page,
    # "强势标的筛选": render_strong_assets_page,
    # "底部抬升筛选": render_bottom_lift_page,
    # "标签化涨跌幅": render_price_change_by_label,
    "自选跟踪": render_watchlist_page,
    "自选标的": render_watchlist_page,
}
# 动态添加
PAGES["编辑标的标签"] = render_label_assets_page

FINGERPRINT_FILE = Path("data/fingerprints.json")


def load_fingerprints() -> dict:
    if FINGERPRINT_FILE.exists():
        try:
            return json.loads(FINGERPRINT_FILE.read_text())
        except Exception:
            pass
    return {}


def save_fingerprints(fp_dict: dict) -> None:
    FINGERPRINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    FINGERPRINT_FILE.write_text(json.dumps(fp_dict))

def require_login() -> bool:
    """Login with optional fingerprint auto-login."""

    # Generate a stable device fingerprint based on non-sensitive
    # information and ensure it is available as the ``fp`` query
    # parameter.  The MD5 value is also cached in ``localStorage`` so
    # future visits reuse the same token even after closing the
    # browser.
    st.components.v1.html(
        """
        <script>
        (function() {
            const params = new URLSearchParams(window.location.search);
            const info = navigator.userAgent + (navigator.language||'') +
                         (navigator.platform||'') + screen.width + 'x' + screen.height;

            function md5cycle(x,k){var a=x[0],b=x[1],c=x[2],d=x[3];
            a=ff(a,b,c,d,k[0],7,-680876936);d=ff(d,a,b,c,k[1],12,-389564586);
            c=ff(c,d,a,b,k[2],17,606105819);b=ff(b,c,d,a,k[3],22,-1044525330);
            a=ff(a,b,c,d,k[4],7,-176418897);d=ff(d,a,b,c,k[5],12,1200080426);
            c=ff(c,d,a,b,k[6],17,-1473231341);b=ff(b,c,d,a,k[7],22,-45705983);
            a=ff(a,b,c,d,k[8],7,1770035416);d=ff(d,a,b,c,k[9],12,-1958414417);
            c=ff(c,d,a,b,k[10],17,-42063);b=ff(b,c,d,a,k[11],22,-1990404162);
            a=ff(a,b,c,d,k[12],7,1804603682);d=ff(d,a,b,c,k[13],12,-40341101);
            c=ff(c,d,a,b,k[14],17,-1502002290);b=ff(b,c,d,a,k[15],22,1236535329);
            a=gg(a,b,c,d,k[1],5,-165796510);d=gg(d,a,b,c,k[6],9,-1069501632);
            c=gg(c,d,a,b,k[11],14,643717713);b=gg(b,c,d,a,k[0],20,-373897302);
            a=gg(a,b,c,d,k[5],5,-701558691);d=gg(d,a,b,c,k[10],9,38016083);
            c=gg(c,d,a,b,k[15],14,-660478335);b=gg(b,c,d,a,k[4],20,-405537848);
            a=gg(a,b,c,d,k[9],5,568446438);d=gg(d,a,b,c,k[14],9,-1019803690);
            c=gg(c,d,a,b,k[3],14,-187363961);b=gg(b,c,d,a,k[8],20,1163531501);
            a=gg(a,b,c,d,k[13],5,-1444681467);d=gg(d,a,b,c,k[2],9,-51403784);
            c=gg(c,d,a,b,k[7],14,1735328473);b=gg(b,c,d,a,k[12],20,-1926607734);
            a=hh(a,b,c,d,k[5],4,-378558);d=hh(d,a,b,c,k[8],11,-2022574463);
            c=hh(c,d,a,b,k[11],16,1839030562);b=hh(b,c,d,a,k[14],23,-35309556);
            a=hh(a,b,c,d,k[1],4,-1530992060);d=hh(d,a,b,c,k[4],11,1272893353);
            c=hh(c,d,a,b,k[7],16,-155497632);b=hh(b,c,d,a,k[10],23,-1094730640);
            a=hh(a,b,c,d,k[13],4,681279174);d=hh(d,a,b,c,k[0],11,-358537222);
            c=hh(c,d,a,b,k[3],16,-722521979);b=hh(b,c,d,a,k[6],23,76029189);
            a=hh(a,b,c,d,k[9],4,-640364487);d=hh(d,a,b,c,k[12],11,-421815835);
            c=hh(c,d,a,b,k[15],16,530742520);b=hh(b,c,d,a,k[2],23,-995338651);
            a=ii(a,b,c,d,k[0],6,-198630844);d=ii(d,a,b,c,k[7],10,1126891415);
            c=ii(c,d,a,b,k[14],15,-1416354905);b=ii(b,c,d,a,k[5],21,-57434055);
            a=ii(a,b,c,d,k[12],6,1700485571);d=ii(d,a,b,c,k[3],10,-1894986606);
            c=ii(c,d,a,b,k[10],15,-1051523);b=ii(b,c,d,a,k[1],21,-2054922799);
            a=ii(a,b,c,d,k[8],6,1873313359);d=ii(d,a,b,c,k[15],10,-30611744);
            c=ii(c,d,a,b,k[6],15,-1560198380);b=ii(b,c,d,a,k[13],21,1309151649);
            a=ii(a,b,c,d,k[4],6,-145523070);d=ii(d,a,b,c,k[11],10,-1120210379);
            c=ii(c,d,a,b,k[2],15,718787259);b=ii(b,c,d,a,k[9],21,-343485551);
            x[0]=add32(a,x[0]);x[1]=add32(b,x[1]);x[2]=add32(c,x[2]);x[3]=add32(d,x[3]);}
            function cmn(q,a,b,x,s,t){a=add32(add32(a,q),add32(x,t));return add32((a<<s)|(a>>> (32-s)),b);}
            function ff(a,b,c,d,x,s,t){return cmn((b&c)|((~b)&d),a,b,x,s,t);}function gg(a,b,c,d,x,s,t){return cmn((b&d)|(c&(~d)),a,b,x,s,t);}function hh(a,b,c,d,x,s,t){return cmn(b^c^d,a,b,x,s,t);}function ii(a,b,c,d,x,s,t){return cmn(c^(b|(~d)),a,b,x,s,t);}
            function md51(s){var n=s.length,state=[1732584193,-271733879,-1732584194,271733878],i;for(i=64;i<=s.length;i+=64){md5cycle(state,md5blk(s.substring(i-64,i)));}s=s.substring(i-64);var tail=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];for(i=0;i<s.length;i++)tail[i>>2]|=s.charCodeAt(i)<<((i%4)<<3);tail[i>>2]|=0x80<<((i%4)<<3);if(i>55){md5cycle(state,tail);for(i=0;i<16;i++)tail[i]=0;}tail[14]=n*8;md5cycle(state,tail);return state;}
            function md5blk(s){var md5blks=[],i;for(i=0;i<64;i+=4){md5blks[i>>2]=s.charCodeAt(i)+(s.charCodeAt(i+1)<<8)+(s.charCodeAt(i+2)<<16)+(s.charCodeAt(i+3)<<24);}return md5blks;}
            var hex_chr="0123456789abcdef".split("");function rhex(n){var s="",j=0;for(;j<4;j++)s+=hex_chr[(n>>>(j*8+4))&15]+hex_chr[(n>>>(j*8))&15];return s;}
            function hex(x){for(var i=0;i<x.length;i++)x[i]=rhex(x[i]);return x.join("");}
            function md5(s){return hex(md51(s));}
            function add32(a,b){return(a+b)&0xFFFFFFFF;}
            const hash = md5(info);
            try {
                if (window.localStorage.getItem('deviceId') !== hash) {
                    window.localStorage.setItem('deviceId', hash);
                }
            } catch (e) {}
            if (params.get('fp') !== hash) {
                params.set('fp', hash);
                window.location.search = params.toString();
            }
        })();
        </script>
        """,
        height=0,
    )

    # Try automatic login via fingerprint in query params
    params = st.query_params
    fp_param = params.get("fp")
    fingerprints = load_fingerprints()
    if fp_param:
        for name, fp in fingerprints.items():
            if fp == fp_param:
                st.session_state["logged_in"] = True
                st.session_state["username"] = name
                return True

    if st.session_state.get("logged_in"):
        return True

    st.title("登录")
    u = st.text_input("用户名")
    p = st.text_input("密码", type="password")
    if st.button("登录"):
        if USER_CREDENTIALS.get(u) == p:
            existing_fp = fingerprints.get(u)
            if existing_fp and existing_fp != fp_param:
                st.error("ERROR 01")
                return False
            if not existing_fp:
                fp_param = fp_param or hashlib.md5(uuid4().hex.encode()).hexdigest()
                fingerprints[u] = fp_param
                save_fingerprints(fingerprints)
            st.session_state["logged_in"] = True
            st.session_state["username"] = u
            safe_rerun()
        else:
            st.error("用户名或密码错误")
    return False

def main():
    st.set_page_config(page_title="K2Database Monitor", layout="wide")
    if not require_login():
        return

    if st.sidebar.button("退出登录"):
        st.session_state.clear()
        safe_rerun()

    choice = st.sidebar.radio("选择页面", list(PAGES.keys()))
    PAGES[choice]()

if __name__ == "__main__":
    main()
