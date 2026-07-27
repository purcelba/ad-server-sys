"""Debug UI entrypoint: `st.tabs(["Rider", "Ops"])`. Run with
`make ui` (`uv run streamlit run adserver/ui/app.py`) once the rest of
the stack is up - see README.md for exactly which services need to be
running first.
"""

from __future__ import annotations

import streamlit as st

from adserver.ui import ops_tab, rider_tab

st.set_page_config(page_title="Toy Ad Server - Debug UI", layout="wide")

rider, ops = st.tabs(["Rider", "Ops"])
with rider:
    rider_tab.render()
with ops:
    ops_tab.render()
