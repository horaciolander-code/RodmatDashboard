import requests
import streamlit as st
from config import API_BASE_URL


def api_get(endpoint: str, params: dict = None) -> dict | list | None:
    token = st.session_state.get("jwt_token")
    if not token:
        return None
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(f"{API_BASE_URL}{endpoint}", headers=headers, params=params, timeout=60)
        if r.status_code == 401:
            st.session_state.pop("jwt_token", None)
            st.error("Session expired. Please log in again.")
            st.rerun()
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API error: {e}")
        return None


def api_post(endpoint: str, json_data: dict = None, files: dict = None) -> dict | None:
    token = st.session_state.get("jwt_token")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.post(f"{API_BASE_URL}{endpoint}", headers=headers, json=json_data,
                          files=files, timeout=300)
        if r.status_code == 401:
            st.session_state.pop("jwt_token", None)
            st.error("Session expired. Please log in again.")
            st.rerun()
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API error: {e}")
        return None


def api_put(endpoint: str, json_data: dict = None) -> dict | None:
    token = st.session_state.get("jwt_token")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.put(f"{API_BASE_URL}{endpoint}", headers=headers, json=json_data, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API error: {e}")
        return None


def api_delete(endpoint: str) -> bool:
    token = st.session_state.get("jwt_token")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.delete(f"{API_BASE_URL}{endpoint}", headers=headers, timeout=30)
        r.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"API error: {e}")
        return False


def login(email: str, password: str) -> bool:
    try:
        r = requests.post(f"{API_BASE_URL}/auth/login",
                          data={"username": email, "password": password}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            st.session_state["jwt_token"] = data["access_token"]
            return True
        return False
    except requests.exceptions.RequestException:
        return False


def register(email: str, password: str, store_name: str) -> bool:
    try:
        r = requests.post(f"{API_BASE_URL}/auth/register",
                          json={"email": email, "password": password, "store_name": store_name},
                          timeout=10)
        if r.status_code == 201:
            data = r.json()
            st.session_state["jwt_token"] = data["access_token"]
            return True
        if r.status_code == 409:
            st.error("Email already registered")
        return False
    except requests.exceptions.RequestException as e:
        st.error(f"Registration error: {e}")
        return False
