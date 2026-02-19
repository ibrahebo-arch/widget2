import pandas as pd
import numpy as np
import streamlit as st
import os
import json
import sqlite3
import threading
import time
from datetime import date, timedelta, datetime
from contextlib import contextmanager

# ---------------- CONFIG ----------------
DB_PATH = r"D:\RA APP\AI\widget2\Bundlepurchase_Merged.db"
PARQUET_PATH = r'Bundlepurchase_Merged.parquet'
DB_TABLE = 'bundlepurchase_hourly'
BASE_DIR = os.path.dirname(__file__)
REFRESH_INTERVAL = 3600  # 60 minutes in seconds

st.set_page_config(page_title="Advanced Purchases analyzer", layout="wide")

# Initialize session state for tracking
if 'last_db_mod_time' not in st.session_state:
    st.session_state.last_db_mod_time = 0
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = datetime.now()
if 'last_log_hour' not in st.session_state:
    st.session_state.last_log_hour = -1
if 'progress_callback' not in st.session_state:
    st.session_state.progress_callback = None
if 'manual_run_active' not in st.session_state:
    st.session_state.manual_run_active = False
if 'auto_enabled' not in st.session_state:
    st.session_state.auto_enabled = True

# ============ SAFE DATABASE CONNECTION HELPER ============
@contextmanager
def get_db_connection():
    """Context manager for safe database connections with timeout and isolation"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
        conn.isolation_level = None  # Autocommit mode
        yield conn
    except sqlite3.DatabaseError as e:
        st.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Detect database file modifications (10-minute updates)
def check_db_modified():
    """Check if database file has been modified since last check"""
    try:
        current_mod_time = os.path.getmtime(DB_PATH)
        if current_mod_time > st.session_state.last_db_mod_time:
            st.session_state.last_db_mod_time = current_mod_time
            st.session_state.last_refresh_time = datetime.now()
            return True
    except OSError:
        pass
    return False

# ============ AUTO DATABASE MONITORING & LOGGING ============
def load_processed_keys(filename):
    """Load all keys that have already been processed and logged"""
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip() and not line.startswith('---') and not line.startswith('===='))
    except:
        pass
    return set()

def save_processed_key(filename, key):
    """Append a new processed key to the tracking file"""
    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(key + '\n')
    except:
        pass

# ============ AUTO PARQUET REFRESH (EVERY HOUR) ============
def refresh_parquet_file():
    """Delete old parquet and recreate from database every hour"""
    try:
        # Delete existing parquet if exists
        if os.path.exists(PARQUET_PATH):
            os.remove(PARQUET_PATH)
        
        # Reload from database
        with get_db_connection() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {DB_TABLE}", conn)
        
        if not df.empty:
            df.to_parquet(PARQUET_PATH, engine="pyarrow", compression="snappy")
    except Exception as e:
        pass

def auto_detect_and_log_new_records(progress_callback=None):
    """Auto-detect new records, calculate alerts (>1.2x), and log to files
    
    Args:
        progress_callback: Optional callable to update progress. Called as progress_callback(current, total, message)
    """
    try:
        if progress_callback:
            progress_callback(0, 100, "Connecting to database...")
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Only process the previous completed hour.
        now = datetime.now()
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        eligible_hour = current_hour_start - timedelta(hours=1)
        eligible_str = eligible_hour.strftime('%Y-%m-%d %H:00:00')

        # If DB has no rows for the eligible (just-finished) hour, skip and wait
        cursor.execute(f"SELECT COUNT(1) FROM {DB_TABLE} WHERE hour_start = ?", (eligible_str,))
        try:
            count = cursor.fetchone()[0]
        except:
            count = 0

        if not count:
            conn.close()
            return

        # Fetch rows only for the completed hour (stable results)
        cursor.execute(f"SELECT * FROM {DB_TABLE} WHERE hour_start = ? ORDER BY hour_start DESC", (eligible_str,))
        columns = [description[0] for description in cursor.description]
        new_records = cursor.fetchall()
        conn.close()
        
        if progress_callback:
            progress_callback(10, 100, f"Found {len(new_records)} records for {eligible_str}...")
        
        tracking_file = os.path.join(BASE_DIR, ".auto_logged_records.txt")
        
        try:
            with open(tracking_file, 'r', encoding='utf-8') as f:
                logged_records = set(line.strip() for line in f if line.strip())
        except:
            logged_records = set()
        
        new_df = pd.DataFrame(new_records, columns=columns)
        new_df['hour_start'] = pd.to_datetime(new_df['hour_start'])
        new_df['date'] = new_df['hour_start'].dt.date
        new_df['hour_of_day'] = new_df['hour_start'].dt.hour
        new_df['day_name'] = new_df['hour_start'].dt.day_name()
        
        # Load full data for averages
        try:
            if os.path.exists(PARQUET_PATH):
                full_df = pd.read_parquet(PARQUET_PATH)
            else:
                conn = sqlite3.connect(DB_PATH)
                full_df = pd.read_sql_query(f"SELECT * FROM {DB_TABLE}", conn)
                conn.close()
            
            full_df['hour_start'] = pd.to_datetime(full_df['hour_start'])
            full_df['day_name'] = full_df['hour_start'].dt.day_name()
            full_df['hour_of_day'] = full_df['hour_start'].dt.hour
        except:
            full_df = None
        
        new_entries = []
        total_rows = len(new_df)
        for idx, row in new_df.iterrows():
            if progress_callback and idx % max(1, total_rows // 10) == 0:
                progress_callback(10 + int((idx / total_rows) * 30), 100, f"Processing record {idx+1}/{total_rows}...")
            erp = row.get('ERP_NAME', 'N/A')
            bundle = row.get('BUNDLE_NAME', 'N/A')
            site = row.get('SITE_NAME', 'N/A')
            invokedby = row.get('INVOKEDBY', 'N/A')
            erp_count = row.get('erp_count', 0)
            hour_of_day = row.get('hour_of_day')
            day_name = row.get('day_name')
            
            record_key = f"{erp}|{bundle}|{site}|{invokedby}"
            
            if record_key not in logged_records:
                erp_qty = erp_count
                in_qty = erp_count
                site_qty = erp_count
                invokedby_qty = erp_count
                
                avg_erp, avg_bundle, avg_site, source_app = 0, 0, 0, 0
                if full_df is not None and day_name and hour_of_day is not None:
                    hist_erp = full_df[(full_df['ERP_NAME'] == erp) & 
                                      (full_df['day_name'] == day_name) & 
                                      (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                    avg_erp = hist_erp if pd.notna(hist_erp) and hist_erp > 0 else 0
                    
                    hist_bundle = full_df[(full_df['BUNDLE_NAME'] == bundle) & 
                                         (full_df['day_name'] == day_name) & 
                                         (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                    avg_bundle = hist_bundle if pd.notna(hist_bundle) and hist_bundle > 0 else 0
                    
                    hist_site = full_df[(full_df['SITE_NAME'] == site) & 
                                       (full_df['day_name'] == day_name) & 
                                       (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                    avg_site = hist_site if pd.notna(hist_site) and hist_site > 0 else 0
                    
                    hist_src = full_df[(full_df['INVOKEDBY'] == invokedby) & 
                                      (full_df['day_name'] == day_name) & 
                                      (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                    source_app = hist_src if pd.notna(hist_src) and hist_src > 0 else 0
                
                def calc_ratio(current, baseline):
                    if current == 0:
                        return 0
                    return round(abs((current - baseline) / current), 4)
                
                ratio_erp = calc_ratio(erp_qty, avg_erp)
                ratio_other = calc_ratio(in_qty, avg_bundle)
                ratio_site = calc_ratio(site_qty, avg_site)
                ratio_source = calc_ratio(invokedby_qty, source_app)
                
                avg_sum = avg_erp + avg_bundle + avg_site + source_app
                qty_sum = erp_qty + in_qty + site_qty + invokedby_qty
                sensory_ratio = round(abs((avg_sum - qty_sum) / avg_sum), 4) if avg_sum > 0 else 0
                
                # ALERT STATUS (Same as UI: >1.2x baseline = ALERT)
                alert_erp = "🚨 ALERT" if erp_qty > (avg_erp * 1.2) else "✅ NORMAL"
                alert_bundle = "🚨 ALERT" if in_qty > (avg_bundle * 1.2) else "✅ NORMAL"
                alert_site = "🚨 ALERT" if site_qty > (avg_site * 1.2) else "✅ NORMAL"
                alert_source = "🚨 ALERT" if invokedby_qty > (source_app * 1.2) else "✅ NORMAL"
                
                new_entries.append({
                    'key': record_key,
                    'row': row,
                    'erp_qty': erp_qty,
                    'in_qty': in_qty,
                    'site_qty': site_qty,
                    'invokedby_qty': invokedby_qty,
                    'avg_erp': avg_erp,
                    'avg_bundle': avg_bundle,
                    'avg_site': avg_site,
                    'source_app': source_app,
                    'ratio_erp': ratio_erp,
                    'ratio_other': ratio_other,
                    'ratio_site': ratio_site,
                    'ratio_source': ratio_source,
                    'sensory_ratio': sensory_ratio,
                    'alert_erp': alert_erp,
                    'alert_bundle': alert_bundle,
                    'alert_site': alert_site,
                    'alert_source': alert_source
                })
        
        if progress_callback:
            progress_callback(40, 100, "Preparing to write log files...")
        
        if new_entries:
            now_timestamp = datetime.now()
            
            if progress_callback:
                progress_callback(50, 100, "Writing alerts history...")
            
            # 1. ALERTS_HISTORY.TXT - Real-Time Alert Analysis
            alerts_file = os.path.join(BASE_DIR, "alerts_history.txt")
            with open(alerts_file, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*200}\n")
                f.write(f"🟢 NEW - Real-Time Alert Analysis - Auto-Synced {len(new_entries)} Records\n")
                f.write(f"Timestamp: {now_timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'='*200}\n")
                
                extended_cols = list(columns) + ['date', 'hour_of_day', 'day_name',
                                                  'erp_QTY', 'IN_QTY', 'SITE_QTY', 'INVOKEDBY_QTY',
                                                  'Avg_ERP', 'Avg_Bundle', 'Avg_Site', 'Source_App',
                                                  'ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE',
                                                  'Ratio_ERP', 'Ratio_Other', 'Ratio_Site', 'Ratio_Source', 'Sensory_Ratio']
                header = "\t".join(extended_cols)
                f.write(f"{header}\n")
                
                for entry in new_entries:
                    row = entry['row']
                    row_values = []
                    
                    for col in columns:
                        val = row.get(col, '')
                        row_values.append(str(val) if val is not None and val != '' else '')
                    
                    row_values.extend([
                        str(row.get('date', '')),
                        str(row.get('hour_of_day', '')),
                        str(row.get('day_name', '')),
                        str(entry['erp_qty']),
                        str(entry['in_qty']),
                        str(entry['site_qty']),
                        str(entry['invokedby_qty']),
                        str(round(entry['avg_erp'], 4)),
                        str(round(entry['avg_bundle'], 4)),
                        str(round(entry['avg_site'], 4)),
                        str(round(entry['source_app'], 4)),
                        str(entry['alert_erp']),
                        str(entry['alert_bundle']),
                        str(entry['alert_site']),
                        str(entry['alert_source']),
                        str(entry['ratio_erp']),
                        str(entry['ratio_other']),
                        str(entry['ratio_site']),
                        str(entry['ratio_source']),
                        str(entry['sensory_ratio'])
                    ])
                    
                    row_str = "\t".join(row_values)
                    f.write(f"{row_str}\n")
                
                f.write(f"{'='*200}\n")
            
            if progress_callback:
                progress_callback(65, 100, "Writing missing sources history...")
            
            # 2. MISSING_SOURCES_HISTORY.TXT
            missing_file = os.path.join(BASE_DIR, "missing_sources_history.txt")
            with open(missing_file, 'a', encoding='utf-8') as f:
                for entry in new_entries:
                    row = entry['row']
                    bundle = row.get('BUNDLE_NAME', 'N/A')
                    erp = row.get('ERP_NAME', 'N/A')
                    site = row.get('SITE_NAME', 'N/A')
                    invokedby = row.get('INVOKEDBY', 'N/A')
                    date_str = str(row.get('date', ''))
                    hour_num = row.get('hour_of_day', 0)
                    
                    categories = [
                        (f"BUNDLE_NAME|{bundle}|{date_str}|{hour_num}", 'BUNDLE_NAME', bundle, entry['avg_bundle']),
                        (f"ERP_NAME|{erp}|{date_str}|{hour_num}", 'ERP_NAME', erp, entry['avg_erp']),
                        (f"SITE_NAME|{site}|{date_str}|{hour_num}", 'SITE_NAME', site, entry['avg_site']),
                        (f"INVOKEDBY|{invokedby}|{date_str}|{hour_num}", 'INVOKEDBY', invokedby, entry['source_app'])
                    ]
                    
                    for key_str, category, item, hist_avg in categories:
                        if item and item != 'N/A':
                            f.write(f"\n[🟢 NEW] {key_str}\n")
                            f.write(f"  Category: {category}\n")
                            f.write(f"  Item: {item}\n")
                            f.write(f"  Date: {date_str} | Hour: {hour_num}\n")
                            f.write(f"  Historical Avg: {round(hist_avg, 4)}\n")
                            f.write(f"  First Seen: {now_timestamp.isoformat()}\n")
                            f.write(f"  Last Seen: {now_timestamp.isoformat()}\n")
                            f.write(f"{'-'*200}\n")
            
            with open(tracking_file, 'a', encoding='utf-8') as f:
                for entry in new_entries:
                    f.write(entry['key'] + '\n')
    
    except Exception as e:
        if progress_callback:
            progress_callback(100, 100, "Completed")
        pass

# Background auto-refresh and auto-logging system
def start_background_scheduler():
    """Monitors DB changes and auto-logs analysis results"""
    def scheduler_loop():
        last_parquet_hour = None
        
        while True:
            try:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute
                current_second = now.second
                
                # Refresh parquet file every hour (at minute 0, second 0)
                if current_hour != last_parquet_hour and current_minute == 0 and current_second < 5:
                    refresh_parquet_file()
                    # Rebuild today's logs up to the last completed hour
                    try:
                        rebuild_today_logs_for_completed_hours()
                    except:
                        pass
                    last_parquet_hour = current_hour
                
                # Only run auto-detect if auto is enabled
                if st.session_state.get('auto_enabled', True):
                    auto_detect_and_log_new_records()
                
                # Check if DB was modified
                if check_db_modified():
                    st.rerun()
                
                # Trigger auto-logging at end of hour
                if current_minute == 59 and current_second >= 55 and current_hour != st.session_state.last_log_hour:
                    st.session_state.last_log_hour = current_hour
                    trigger_auto_logging()
                    time.sleep(10)
                
                time.sleep(180)  # Check every 3 minutes
            except Exception as e:
                pass
            
    if 'scheduler_started' not in st.session_state:
        scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        scheduler_thread.start()
        st.session_state.scheduler_started = True

def rebuild_today_logs_for_completed_hours():
    """Rebuild today's log entries up to the last completed hour.
    Removes existing lines for today's date from the two history files
    and appends regenerated results from the DB for hours that have completed.
    """
    try:
        now = datetime.now()
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        eligible_hour = current_hour_start - timedelta(hours=1)
        date_str = eligible_hour.strftime('%Y-%m-%d')
        eligible_cutoff = eligible_hour.strftime('%Y-%m-%d %H:00:00')

        # Load rows for today up to eligible hour
        conn = sqlite3.connect(DB_PATH)
        query = f"SELECT * FROM {DB_TABLE} WHERE substr(hour_start,1,10)=? AND hour_start <= ? ORDER BY hour_start ASC"
        df_today = pd.read_sql_query(query, conn, params=(date_str, eligible_cutoff))
        conn.close()

        if df_today.empty:
            return

        # Prepare full_df for historical averages (use parquet if available)
        try:
            if os.path.exists(PARQUET_PATH):
                full_df = pd.read_parquet(PARQUET_PATH)
            else:
                conn = sqlite3.connect(DB_PATH)
                full_df = pd.read_sql_query(f"SELECT * FROM {DB_TABLE}", conn)
                conn.close()
            full_df['hour_start'] = pd.to_datetime(full_df['hour_start'])
            full_df['day_name'] = full_df['hour_start'].dt.day_name()
            full_df['hour_of_day'] = full_df['hour_start'].dt.hour
        except:
            full_df = None

        # Calculate derived values per row
        df_today['hour_start'] = pd.to_datetime(df_today['hour_start'])
        df_today['date'] = df_today['hour_start'].dt.date
        df_today['hour_of_day'] = df_today['hour_start'].dt.hour
        df_today['day_name'] = df_today['hour_start'].dt.day_name()

        generated_alert_rows = []
        generated_missing_blocks = []

        for idx, row in df_today.iterrows():
            erp = row.get('ERP_NAME', 'N/A')
            bundle = row.get('BUNDLE_NAME', 'N/A')
            site = row.get('SITE_NAME', 'N/A')
            invokedby = row.get('INVOKEDBY', 'N/A')
            erp_count = row.get('erp_count', 0)
            day_name = row.get('day_name')
            hour_of_day = row.get('hour_of_day')

            # Quantities
            erp_qty = erp_count
            in_qty = erp_count
            site_qty = erp_count
            invokedby_qty = erp_count

            # Historical averages
            avg_erp = avg_bundle = avg_site = source_app = 0
            if full_df is not None and day_name and hour_of_day is not None:
                hist_erp = full_df[(full_df['ERP_NAME'] == erp) & (full_df['day_name'] == day_name) & (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                avg_erp = hist_erp if pd.notna(hist_erp) and hist_erp > 0 else 0
                hist_bundle = full_df[(full_df['BUNDLE_NAME'] == bundle) & (full_df['day_name'] == day_name) & (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                avg_bundle = hist_bundle if pd.notna(hist_bundle) and hist_bundle > 0 else 0
                hist_site = full_df[(full_df['SITE_NAME'] == site) & (full_df['day_name'] == day_name) & (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                avg_site = hist_site if pd.notna(hist_site) and hist_site > 0 else 0
                hist_src = full_df[(full_df['INVOKEDBY'] == invokedby) & (full_df['day_name'] == day_name) & (full_df['hour_of_day'] == hour_of_day)]['erp_count'].mean()
                source_app = hist_src if pd.notna(hist_src) and hist_src > 0 else 0

            def calc_ratio(current, baseline):
                if current == 0:
                    return 0
                return round(abs((current - baseline) / current), 4)

            ratio_erp = calc_ratio(erp_qty, avg_erp)
            ratio_other = calc_ratio(in_qty, avg_bundle)
            ratio_site = calc_ratio(site_qty, avg_site)
            ratio_source = calc_ratio(invokedby_qty, source_app)

            avg_sum = avg_erp + avg_bundle + avg_site + source_app
            qty_sum = erp_qty + in_qty + site_qty + invokedby_qty
            sensory_ratio = round(abs((avg_sum - qty_sum) / avg_sum), 4) if avg_sum > 0 else 0

            # Alerts
            alert_erp = "🚨 ALERT" if (avg_erp > 0 and erp_qty > (avg_erp * 1.2)) else "✅ NORMAL"
            alert_bundle = "🚨 ALERT" if (avg_bundle > 0 and in_qty > (avg_bundle * 1.2)) else "✅ NORMAL"
            alert_site = "🚨 ALERT" if (avg_site > 0 and site_qty > (avg_site * 1.2)) else "✅ NORMAL"
            alert_source = "🚨 ALERT" if (source_app > 0 and invokedby_qty > (source_app * 1.2)) else "✅ NORMAL"

            # Build alert row (tab-separated)
            base_cols = ['ERP_NAME','BUNDLE_NAME','SITE_NAME','INVOKEDBY','hour_start','erp_count']
            base_vals = [str(row.get(c,'')) for c in base_cols]
            extended_vals = [
                str(row.get('date','')),
                str(hour_of_day),
                str(day_name),
                str(erp_qty),
                str(in_qty),
                str(site_qty),
                str(invokedby_qty),
                str(round(avg_erp,4)),
                str(round(avg_bundle,4)),
                str(round(avg_site,4)),
                str(round(source_app,4)),
                alert_erp,
                alert_bundle,
                alert_site,
                alert_source,
                str(ratio_erp),
                str(ratio_other),
                str(ratio_site),
                str(ratio_source),
                str(sensory_ratio)
            ]
            generated_alert_rows.append('\t'.join(base_vals + extended_vals))

            # Build missing sources block
            hist_avg_value = avg_bundle if avg_bundle>0 else (avg_erp if avg_erp>0 else (avg_site if avg_site>0 else source_app))
            block = f"\n[🟢 NEW] BUNDLE_NAME|{bundle}|{date_str}|{hour_of_day}\n  Category: BUNDLE_NAME\n  Item: {bundle}\n  Date: {date_str} | Hour: {hour_of_day}\n  Historical Avg: {round(hist_avg_value,4)}\n  First Seen: {now.isoformat()}\n  Last Seen: {now.isoformat()}\n{'-'*200}\n"
            generated_missing_blocks.append(block)

        # Remove existing today's lines from both files (simple heuristic)
        for path in [os.path.join(BASE_DIR,'alerts_history.txt'), os.path.join(BASE_DIR,'missing_sources_history.txt')]:
            try:
                if os.path.exists(path):
                    with open(path,'r',encoding='utf-8') as f:
                        lines = f.readlines()
                    filtered = [ln for ln in lines if date_str not in ln]
                    with open(path,'w',encoding='utf-8') as f:
                        f.writelines(filtered)
            except:
                pass

        # Append regenerated today's alert rows to alerts_history.txt
        try:
            alerts_path = os.path.join(BASE_DIR,'alerts_history.txt')
            with open(alerts_path,'a',encoding='utf-8') as f:
                f.write('\n' + '='*200 + '\n')
                f.write(f"🟢 REBUILT - Daily Auto-Sync up to {eligible_cutoff}\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write('='*200 + '\n')
                header = '\t'.join(base_cols + ['date','hour_of_day','day_name','erp_QTY','IN_QTY','SITE_QTY','INVOKEDBY_QTY','Avg_ERP','Avg_Bundle','Avg_Site','Source_App','ALERT_ERP','ALERT_BUNDLE','ALERT_SITE','ALERT_SOURCE','Ratio_ERP','Ratio_Other','Ratio_Site','Ratio_Source','Sensory_Ratio'])
                f.write(header + '\n')
                for r in generated_alert_rows:
                    f.write(r + '\n')
                f.write('='*200 + '\n')
        except:
            pass

        # Append regenerated missing sources blocks
        try:
            missing_path = os.path.join(BASE_DIR,'missing_sources_history.txt')
            with open(missing_path,'a',encoding='utf-8') as f:
                for b in generated_missing_blocks:
                    f.write(b)
        except:
            pass

    except Exception:
        pass

# Start the background scheduler
start_background_scheduler()

# Show refresh status with DB monitoring
time_since_refresh = (datetime.now() - st.session_state.last_refresh_time).total_seconds()
minutes_since = int(time_since_refresh / 60)
db_status = "🟢 DB Monitored" if st.session_state.last_db_mod_time > 0 else "⚪ Waiting for DB"
st.sidebar.info(f"📊 {db_status}\n⏱️ Last refresh: {minutes_since} min ago\n⚙️ Auto-logging: ACTIVE")

# ---------------- AUTO LOAD / CONVERT ----------------
@st.cache_data
def load_data():
    """Load data from SQLite database and sync to Parquet"""
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {DB_TABLE}", conn)
        conn.close()
        
        if df.empty:
            st.error("❌ Database table is empty or cannot be read.")
            return pd.DataFrame()
        
        # Save to Parquet for faster local access
        df.to_parquet(PARQUET_PATH, engine="pyarrow", compression="snappy")
        st.success("✅ Data loaded from database and synced to Parquet")
        return df
    except sqlite3.Error as e:
        st.error(f"❌ Database Error: {e}")
        # Try to load from parquet as fallback
        if os.path.exists(PARQUET_PATH):
            st.warning("⚠️ Loading cached data from Parquet file...")
            return pd.read_parquet(PARQUET_PATH)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error loading data: {e}")
        return pd.DataFrame()

df = load_data()

# ---------------- HISTORY HELPERS ----------------
def _history_path(filename):
    return os.path.join(BASE_DIR, filename)

def load_seen_set(fname):
    path = _history_path(fname)
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def save_seen_set(fname, keys_set):
    path = _history_path(fname)
    with open(path, "w", encoding="utf-8") as f:
        for k in sorted(keys_set):
            f.write(k + "\n")

def write_status_files(base, new_keys, old_keys):
    new_path = _history_path(f"{base}_new.txt")
    old_path = _history_path(f"{base}_old.txt")
    with open(new_path, "w", encoding="utf-8") as f:
        for k in sorted(new_keys):
            f.write(k + "\n")
    with open(old_path, "w", encoding="utf-8") as f:
        for k in sorted(old_keys):
            f.write(k + "\n")

def load_history_df(fname):
    """Load history from TXT file (or CSV for backward compatibility)"""
    path = _history_path(fname)
    # Try TXT first, then CSV for backward compatibility
    txt_path = path.replace('.csv', '.txt')
    
    if os.path.exists(txt_path):
        try:
            lines = []
            with open(txt_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    cols = ['Key','Category','Item','HistoricalAvg','Date','Hour','Status','FirstSeen','LastSeen','EntryHour']
                    return pd.DataFrame(columns=cols)
                
                for line in content.split('\n'):
                    if line.strip() and not line.startswith('---') and not line.startswith('====') and not line.startswith('History') and not line.startswith('Sorted'):
                        parts = line.split('|')
                        if len(parts) >= 9:
                            lines.append({
                                'Key': parts[0].strip(),
                                'Category': parts[1].strip() if len(parts) > 1 else 'Type',
                                'Item': parts[2].strip() if len(parts) > 2 else parts[3].strip(),
                                'HistoricalAvg': parts[3].strip() if len(parts) > 3 else '',
                                'Date': parts[4].strip() if len(parts) > 4 else '',
                                'Hour': int(parts[5].strip()) if len(parts) > 5 else 0,
                                'Status': parts[6].strip() if len(parts) > 6 else 'OLD',
                                'FirstSeen': parts[7].strip() if len(parts) > 7 else '',
                                'LastSeen': parts[8].strip() if len(parts) > 8 else '',
                                'EntryHour': parts[9].strip() if len(parts) > 9 else ''
                            })
            if lines:
                return pd.DataFrame(lines)
        except Exception as e:
            st.warning(f"Error reading TXT history: {e}")
    
    if os.path.exists(path):
        try:
            return pd.read_csv(path, parse_dates=['FirstSeen','LastSeen'])
        except Exception:
            return pd.read_csv(path)
    
    # empty template
    cols = ['Key','Category','Item','HistoricalAvg','Date','Hour','Status','FirstSeen','LastSeen','EntryHour']
    return pd.DataFrame(columns=cols)

def save_history_df(fname, df):
    """Save history to TXT file (organized by timestamp, newest first)"""
    path = _history_path(fname)
    txt_path = path.replace('.csv', '.txt')
    
    if df.empty:
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write("No history records yet.\n")
        return
    
    # Ensure EntryHour column exists
    if 'EntryHour' not in df.columns:
        df['EntryHour'] = ''
    
    # Check if this is an alerts file (has alert-specific columns)
    is_alerts_file = 'ALERT_ERP' in df.columns
    
    # Sort by LastSeen timestamp (newest first)
    try:
        df['LastSeen_parsed'] = pd.to_datetime(df['LastSeen'], errors='coerce')
        df_sorted = df.sort_values('LastSeen_parsed', ascending=False, na_position='last')
        df_sorted = df_sorted.drop('LastSeen_parsed', axis=1)
    except Exception:
        df_sorted = df
    
    # Write TXT file
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(f"{'='*200}\n")
        f.write(f"History Records - Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"File Type: {'ALERTS' if is_alerts_file else 'MISSING SOURCES'}\n")
        f.write(f"Sorted by: Most Recent First (Newest Entries at Top)\n")
        f.write(f"Legend: 🟢 NEW = First appearance | ⚫ OLD = Seen in previous runs\n")
        f.write(f"{'='*200}\n\n")
        
        # If alerts file, write with tab-separated data format
        if is_alerts_file:
            # Write header
            header_cols = ['ERP_NAME', 'BUNDLE_NAME', 'SITE_NAME', 'INVOKEDBY', 'hour_start',
                          'erp_QTY', 'IN_QTY', 'SITE_QTY', 'INVOKEDBY_QTY',
                          'Avg_ERP', 'Avg_Bundle', 'Avg_Site', 'Source_App',
                          'ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE', 'ALERT_STATUS']
            available_cols = [col for col in header_cols if col in df_sorted.columns]
            f.write('\t'.join(available_cols) + '\n')
            
            # Write data rows
            for idx, row in df_sorted.iterrows():
                row_data = []
                for col in available_cols:
                    val = row.get(col, '')
                    # Format timestamp
                    if col == 'hour_start' and pd.notna(val):
                        val = pd.to_datetime(val).strftime('%Y-%m-%d %H:%M:%S')
                    row_data.append(str(val))
                f.write('\t'.join(row_data) + '\n')
        else:
            # Missing sources format (keep original format)
            for idx, row in df_sorted.iterrows():
                status_marker = '🟢 NEW' if row.get('Status') == 'NEW' else '⚫ OLD'
                entry_hour_str = f" [Hour: {row.get('EntryHour', 'N/A')}]" if row.get('EntryHour') else ""
                f.write(f"[{status_marker}]{entry_hour_str} {row['Key']}\n")
                f.write(f"  Category: {row.get('Category', row.get('Type', 'N/A'))}\n")
                f.write(f"  Item: {row.get('Item', 'N/A')}\n")
                f.write(f"  Date: {row.get('Date', 'N/A')} | Hour: {row.get('Hour', 'N/A')}\n")
                f.write(f"  Historical Avg: {row.get('HistoricalAvg', '0')}\n")
                f.write(f"  First Seen: {row.get('FirstSeen', 'N/A')}\n")
                f.write(f"  Last Seen: {row.get('LastSeen', 'N/A')}\n")
                f.write(f"{'-'*200}\n\n")

# ============ AUTO-LOGGING SYSTEM ============
def create_logs_directory():
    """Create logs directory if it doesn't exist"""
    logs_dir = os.path.join(BASE_DIR, 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    return logs_dir

def get_last_hour_from_db():
    """Get data from the last hour in database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = f"""
        SELECT * FROM {DB_TABLE}
        WHERE hour_start = (SELECT MAX(hour_start) FROM {DB_TABLE})
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df if not df.empty else pd.DataFrame()
    except:
        return pd.DataFrame()

def auto_log_missing_sources(df_input):
    """Automatically log Missing Sources Analysis for the last hour to missing_sources_history.txt"""
    try:
        # If no input, get from database
        if df_input.empty:
            df_input = get_last_hour_from_db()
        
        # Master lists
        MASTER_LISTS = {
            "INVOKEDBY": [
                "webService", "Mediation/FastPay/FastPay_APP", "Mediation/FastPay/FastPay_POS",
                "WSC/FIB/bundle", "WSC/NCC_Balance/bundle", "Mediation/FastPay/FastPay_Kartat",
                "Mediation/FIB/FIB_APP", "no app", "CRM", 
                "Mediation/internal_showrooms/internal_showrooms", "API_Dina", "Dina_API",
                "Abdulla_Test", "API_mohammed"
            ],
            "ERP_NAME": [
                "F10K_50GB_FP_SPMIFI_45DAYS_100316", "F10K_50GB_WSC_SPMIFI_45DAYS_100317", "F15K_350GB_FP_SP_30DAYS_SULY_81",
                "F15K_350GB_WSC_SP_30DAYS_SULY_80", "F15K_60GB_WSC_SPMIFI_45DAY_NONFUP_100279", "F18K_350GB_FP_SP_30DAYS_84",
                "F18K_350GB_FP_SP_30DAYS_FMLY_56", "F18K_350GB_WSC_SP_30DAYS_85", "F18K_350GB_WSC_SP_30DAYS_FMLY_55",
                "F18K_640GB_FP_SP_30DAYS_SULY_72", "F18K_640GB_WSC_SP_30DAYS_SULY_73", "F2.5K_50GB_FP_SP_1DAY_100252",
                "F2.5K_50GB_WSC_SP_1DAY_100253", "F20K_640GB_FP_SP_30DAYS_78", "F20K_640GB_WSC_SP_30DAYS_79",
                "F30K_500GB_FP_SP_60DAYS_52", "F30K_500GB_WSC_SP_60DAYS_97", "F39K_1250GB_WSC_SP_30DAYS_76",
                "F39K_600GB_WSC_MR_30DAYS_70", "F4K_15GB_FP_SP_30DAYS_100196", "F5K_25GB_FP_SP_10DAYS_100251",
                "F5K_25GB_WSC_SP_10DAYS_100250", "F60K_2205GB_FP_SP_105DAYS_58", "F7K_20GB_FP_SP_30DAYS_100198",
                "F7K_20GB_WSC_SPMIFI_30DAY_100307", "F89K_1000GB_FP_SP_30DAYS_100176", "F9K_30GB_FP_SP_30DAYS_100199",
                "F9K_30GB_WSC_SPMIFI_30DAYS_100314", "N10K_30GB_FP_SPMIFI_45DAYS_100227", "N10K_30GB_WSC_SPMIFI_45DAYS_100226",
                "N12K_40GB_FP_SPMIFI_45DAYS_100321", "N12K_40GB_WSC_SPMIFI_45DAYS_100320", "N25K_350GB_FP_SPMIFI_30DAYS_D2GB_502",
                "N25K_350GB_FP_SPMIFI_30DAYS_FMLY_504", "N25K_350GB_WSC_SPMIFI_30DAYS_D2GB_501", "N25K_350GB_WSC_SPMIFI_30DAYS_FMLY_503",
                "N27K_750GB_FP_UB_30D_FUP_H310L350_100282", "N27K_750GB_WSC_UB_30D_FUP_FAMILY_100288", "N27K_750GB_WSC_UB_30D_FUPH310L350_100283",
                "N39K_1125GB_WSC_UB_45D_FUP_DLY3GB_100290", "N5K_50GB_FP_SPMIFI_5DAYS_100262", "N5K_50GB_WSC_SPMIFI_5DAYS_100267",
                "F39K_1250GB_FP_SP_30DAYS_75", "F69K_1200GB_WSC_MR_30DAYS_64", "F69K_2200GB_WSC_SP_30DAYS_67",
                "N20K_200GB_WSC_SPMIFI_30DAYS_100242", "N22K_200GB_WSC_SPMIFI_30DAYS_505", "N75K_1800GB_WSC_SPMIFI_120DAYS_100318",
                "80GBx12M_STAFF_SOG", "F27K_500GB_FP_SP_60DAYS_SULY_50", "N22K_200GB_FP_SPMIFI_30DAYS_506",
                "N29K_610GB_FP_SPMIFI_30DYS_100214_DLY3GB", "F27K_500GB_WSC_SP_60DAYS_SULY_94", "F39K_600GB_FP_MR_30DAYS_69",
                "F169K_2000GB_FP_SP_30DAYS_100177", "N27K_750GB_FP_UB_30D_FUP_FAMILY_100287", "N75K_1800GB_FP_SPMIFI_120DAYS_100319",
                "N39K_1125GB_FP_UB_45D_FUP_FAMILY_100294", "F120K_4725GB_WSC_SP_225DAYS_DLY6G_100328", "F60K_2205GB_WSC_SP_105DAYS_60",
                "N39K_1125GB_FP_UB_45D_FUP_DLY3GB_100286", "F69K_1200GB_FP_MR_30DAYS_63", "F69K_2200GB_FP_SP_30DAYS_66",
                "N29K_610GB_WSC_SPMFI_30DYS_100212_DLY3GB", "N39K_1125GB_WSC_UB_45D_FUP_FAMILY_100293", "Fastlink_Primary_offer_VIP",
                "Fastlink_Primary_offer_CRM", "10GBX12M_SOG", "95GBX12M_GP_SOG", "Corp_Special18K_Unlimited",
                "Fixed_LTE_Router_12M", "corporate_18k_fup_unlimited", "150GBx12M_STAFF_free_traffic",
                "Bugatti_Plus_FUP_12M_new_SP", "Site_Offer", "GW_Staff_60GB", "newroz_Primary_offer_CRM",
                "Fastlink_Primary_offer", "10gb_1d_app_gift", "bundle_10GB_1D_N4G_Comp", "FastPay_Gift",
                "10gb_1d_app_Comp", "F9K_80GB_FP_SPMIFI_30DAYS_SO_508", "F9K_80GB_WSC_SPMIFI_30DAYS_SO_507",
                "Zero_balance_bundle_test", "corp_unlimited_profile", "80GBx12M_Fastsim_Dealer",
                "1GBx12M_Monthly_Bundle_SOG", "20GBX12M_GP_SOG", "2GBX12M_GP_SOG", "30GBX12M_Hura_TV",
                "5GBX12M_GP_SOG", "95GBx12M_corporate", "Bugatti_Plus_FUP_12M_new_MiFi",
                "Ferrari_Plus_Unlimited_12M_SOG_SP", "kits_corp_130gb_12m", "newroz_home_35k_12m",
                "router_voip_12m", "F13.5K_200GB_WSC_SP_30DAYS_88", "compansate_20GB_13_Days",
                "Newroz_13Days_comp", "VIP_40GBx6M_SOG", "1GBx12M_FastPay_POS", "F18K_150GB_FP_SP_30DAYS_100230",
                "F6.75K_20GB_FIB_SPMIFI_30DAYS_100339"
            ],
            "BUNDLE_NAME": [
                "6_Months_Fastpay_FUP_SP", "Economic_22K_FUP_new_F", "Economic_25K_FUP_new_F", "Economic_25K_FUP_new_W",
                "Family_new_bundle_FP", "Family_New_Bundle_WS", "Newroz_4.5G_20K_WSC", "Vol_fastlink_18K_FUP_l3m_FP_high",
                "Vol_fastlink_18K_FUP_l3m_WSC_high", "Vol_Suly_15K_FUP_l3m_FP_high", "Vol_Suly_15K_FUP_l3m_WSC_high",
                "Vol_Two_Months_30K_high", "Vol_Two_Months_30K_l2m_ee_high", "Bundle_50GB_Suly_FP", "Bundle_50GB_Suly_WSC",
                "Suly_15K_FUP_l3m_FP_SP", "Suly_15K_FUP_l3m_WSC_SP", "porsche_new_nonfup_wsc", "fastlink_18K_FUP_l3m_FP_SP",
                "family_18K_FUP_l3m_ee_SP", "fastlink_18K_FUP_l3m_WSC_SP", "family_18K_FUP_l3m_WSC_SP", "Suly_18K_social_l4m_FP_SP",
                "Suly_18K_social_l4m_WSC_SP", "Daily_50GB_Fastpay_SP", "Daily_50GB_WSC_SP", "fastlink_20K_social_l4m_FP_SP",
                "fastlink_20K_social_l4m_WSC_SP", "Two_Months_30K_l2m_ee_SP", "Two_Months_30K_WSC_l2m_SP",
                "fastlink_39K_social_l4m_WSC_SP", "fastlink_39K_social_l4m_WSC_MR", "FTTH_Bundle_15GB_SP",
                "10_Days_NonFUP_Fastpay_25Gb_SP", "10_Days_NonFUP_WSC_SP", "fastlink_60K_social_l4m_ee_SP", "cml_20GB_Fastpay",
                "cml_20GB_WSC", "cml_30GB_Fastpay", "Package_9k_30GB_WSC", "Newroz_4G_PLUS_45Days_FP", "Newroz_4G_PLUS_45Days",
                "Newroz_12K_45Days_FP", "Newroz_12K_45Days_WSC", "Economic_25K_FUP_social_ee", "Family_25K_FUP_social_ee",
                "Economic_25K_FUP_social_WSC", "Family_25K_FUP_social_WSC", "newroz_fixed_lte_router_home_27k_FP",
                "newroz_fixed_lte_router_family_27k_WSC", "newroz_fixed_lte_router_home_27k_WSC", "newroz_fixed_lte_39K_router_WSC",
                "5_Days_N4G_Fastpay", "5_Days_N4G_W", "Vol_Two_Months_27K_Suly_ee_high", "fastlink_39K_social_l4m_FP_SP",
                "fastlink_69K_social_l4m_WSC_MR", "fastlink_69K_social_l4m_WSC_SP", "Economic_22K_FUP_social_WSC",
                "Bundle_3_months_plus_one_WSC", "Vol_family_18K_FUP_l3m_WSC_high", "80GBx12M_STAFF_SOG", "Vol_family_18K_FUP_l3m_ee_high",
                "Two_Months_27K_Suly_ee_l2m_SP", "Economic_22K_FUP_social_ee", "Newroz_4G_PLUS_Personal_F",
                "Two_Months_27K_Suly_WSC_l2m_SP", "fastlink_39K_social_l4m_FP_MR", "Vol_3_months_plus_one_WSC_high",
                "12_Months_Fastpay_FUP_SP", "newroz_fixed_lte_router_family_27k_FP", "Bundle_3_months_plus_one_FP",
                "newroz_family_lte_39K_router_Fastpay", "Bundle_6M_plus_45D_WSC_SP", "fastlink_60K_social_l4m_WSC_SP",
                "newroz_fixed_lte_39K_router_Fastpay", "Vol_Two_Months_30K_FIB_l2m_new_high", "fastlink_69K_social_l4m_FP_MR",
                "fastlink_69K_social_l4m_FP_SP", "Newroz_4G_PLUS_Personal", "Vol_3_months_plus_one_FP_high",
                "newroz_family_lte_39K_router_WSC", "Fastlink_Primary_offer", "10GBX12M_SOG", "95GBX12M_GP_SOG",
                "Vol_fastlink_18K_FUP_l3m_FP_high", "Vol_Corp_Special18K_Unlimited_high", "Fixed_LTE_Router_12M",
                "Vol_fastlink_39K_social_l4m_WSC_MR_high", "Vol_Economic_25K_FUP_social_ee_high", "corporate_18k_fup_unlimited",
                "150GBx12M_STAFF_free_traffic", "Bugatti_Plus_FUP_12M_new_SP", "Site_Offer", "Vol_fastlink_69K_social_l4m_FP_MR_high",
                "GW_Staff_60GB", "Vol_10gb_1d_app_gift", "Vol_bundle_10GB_1D_N4G_Comp", "Vol_FastPay_Gift",
                "Vol_fastlink_20K_social_l4m_FP_high", "Bundle_Student_9K_ee", "Bundle_Student_9K_WSC", "newroz_Primary_offer_CRM",
                "Vol_Zero_balance_bundle_test1", "Vol_80GBx12M_Fastsim_Dealer", "1GBx12M_Monthly_Bundle_SOG", "20GBX12M_GP_SOG",
                "2GBX12M_GP_SOG", "30GBX12M_Hura_TV", "5GBX12M_GP_SOG", "Vol_95GBx12M_corporate", "Bugatti_Plus_FUP_12M_new_MiFi",
                "Ferrari_Plus_Unlimited_12M_SOG_SP", "kits_corp_130gb_12m", "newroz_home_35k_12m", "router_voip_12m",
                "Vol_fastlink_18K_FUP_l3m_WSC_high", "Porsche_FUP_Chr_WSC_SP", "Vol_compansate_20GB", "Vol_Newroz_13Days_comp",
                "Vol_Bundle_6M_plus_45D_WSC_1GB_High", "Vol_Bundle_6M_plus_45D_WSC_daily", "Vol_Two_Months_30K_daily",
                "Vol_Two_Months_30K_l2m_ee_daily", "VIP_40GBx6M_SOG", "Vol_1GBx12M_FastPay_POS", "Porsche_FUP_FastPay_new_SP",
                "cml_20GB_FIB"
            ]
        }
        
        # Analyze last hour from database
        target_time = datetime.now() - timedelta(hours=1)
        chk_date = target_time.date()
        chk_hour = target_time.hour
        
        analysis_df = df_input[(df_input['date'] == chk_date) & (df_input['hour_of_day'] == chk_hour)]
        
        # Save to missing_sources_history.txt
        history_file = os.path.join(BASE_DIR, "missing_sources_history.txt")
        
        with open(history_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*200}\n")
            f.write(f"Missing Sources Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Hour: {chk_hour}:00 on {chk_date}\n")
            f.write(f"{'='*200}\n")
            
            if analysis_df.empty:
                f.write(f"⚠️  No records for this hour.\n\n")
                return
            
            for category, master_list in MASTER_LISTS.items():
                active_items = analysis_df[category].unique()
                missing = [item for item in master_list if item not in active_items]
                
                f.write(f"\n🔍 {category} - Missing Items: {len(missing)}\n")
                f.write(f"{'-'*100}\n")
                
                for item in missing:
                    baseline_col = 'Source_App' if category == 'INVOKEDBY' else ('Avg_ERP' if category == 'ERP_NAME' else 'Avg_Bundle')
                    baseline = df_input[df_input[category] == item][baseline_col].max()
                    if baseline > 0:
                        f.write(f"  • {item} | Historical Avg: {baseline:.2f}\n")
                
                if not missing:
                    f.write(f"  ✅ All items active\n")
    except Exception as e:
        pass

def auto_log_alerts(df_input):
    """Automatically log Real-Time Alert Analysis for the last hour to alerts_history.txt"""
    try:
        target_time = datetime.now() - timedelta(hours=1)
        chk_date = target_time.date()
        chk_hour = target_time.hour
        
        current_hour_df = df_input[(df_input['date'] == chk_date) & (df_input['hour_of_day'] == chk_hour)].copy()
        
        # Save to alerts_history.txt
        history_file = os.path.join(BASE_DIR, "alerts_history.txt")
        
        with open(history_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*200}\n")
            f.write(f"Real-Time Alert Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Hour: {chk_hour}:00 on {chk_date}\n")
            f.write(f"{'='*200}\n")
            
            if current_hour_df.empty:
                f.write(f"⚠️  No data found for this hour.\n\n")
                return
            
            # Calculate alerts
            current_hour_df['ALERT_ERP'] = np.where(current_hour_df['erp_QTY'] > (current_hour_df['Avg_ERP'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            current_hour_df['ALERT_BUNDLE'] = np.where(current_hour_df['IN_QTY'] > (current_hour_df['Avg_Bundle'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            current_hour_df['ALERT_SITE'] = np.where(current_hour_df['SITE_QTY'] > (current_hour_df['Avg_Site'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            current_hour_df['ALERT_SOURCE'] = np.where(current_hour_df['INVOKEDBY_QTY'] > (current_hour_df['Source_App'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            
            total_qty = int(current_hour_df['erp_count'].sum())
            alert_count = sum(current_hour_df[col].eq('🚨 ALERT').sum() for col in ['ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE'])
            
            f.write(f"\n📊 Summary: {total_qty:,} Total Units | 🚨 Alerts: {alert_count}\n")
            f.write(f"{'-'*100}\n")
            
            # Log alerts
            for _, row in current_hour_df.iterrows():
                alerts = []
                for col in ['ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE']:
                    if row.get(col) == '🚨 ALERT':
                        alerts.append(col.replace('ALERT_', ''))
                
                if alerts:
                    f.write(f"\n🚨 Alert: {', '.join(alerts)}\n")
                    f.write(f"  ERP: {row.get('ERP_NAME', 'N/A')} | Bundle: {row.get('BUNDLE_NAME', 'N/A')}\n")
                    f.write(f"  Site: {row.get('SITE_NAME', 'N/A')} | Source: {row.get('INVOKEDBY', 'N/A')}\n")
    except Exception as e:
        pass

def trigger_auto_logging():
    """Trigger both Missing Sources and Alert logging"""
    try:
        if not df.empty:
            auto_log_missing_sources(df)
            auto_log_alerts(df)
    except Exception:
        pass

# ============ END AUTO-LOGGING SYSTEM ============

# -------- PREPARE DATE --------

df["hour_start"] = pd.to_datetime(df["hour_start"])
df["date"] = df["hour_start"].dt.date
df["hour_of_day"] = df["hour_start"].dt.hour
df["day_name"] = df["hour_start"].dt.day_name()  # Returns 'Monday', 'Tuesday', etc.

# ---------------- CALCULATIONS (Historical & Current) ----------------

# 1. Create Day/Hour Keys for Historical Grouping
df["day_name"] = df["hour_start"].dt.day_name()
df["hour_of_day"] = df["hour_start"].dt.hour

# 2. Calculate Current Quantities (The 'Now' Values)
df['erp_QTY'] = df.groupby(['ERP_NAME', 'hour_start'])['erp_count'].transform('sum')
df['IN_QTY'] = df.groupby(['BUNDLE_NAME', 'hour_start'])['erp_count'].transform('sum')
df['SITE_QTY'] = df.groupby(['SITE_NAME', 'hour_start'])['erp_count'].transform('sum')

# Special logic for INVOKEDBY_QTY
total_invoked_sum = df.groupby(['INVOKEDBY', 'hour_start'])['erp_count'].transform('sum')
refill_mask = (df['INVOKEDBY'] == 'webService') & (df['BUNDLE_NAME'] == 'Refill')
non_refill_mask = (df['INVOKEDBY'] == 'webService') & (df['BUNDLE_NAME'] != 'Refill')

refill_sum_series = pd.Series(0, index=df.index)
refill_sum_series.loc[refill_mask] = df.loc[refill_mask].groupby(['INVOKEDBY', 'hour_start'])['erp_count'].transform('sum')

non_refill_sum_series = pd.Series(0, index=df.index)
non_refill_sum_series.loc[non_refill_mask] = df.loc[non_refill_mask].groupby(['INVOKEDBY', 'hour_start'])['erp_count'].transform('sum')

df['INVOKEDBY_QTY'] = np.where(
    df['INVOKEDBY'] != 'webService',
    total_invoked_sum,
    np.where(df['BUNDLE_NAME'] == 'Refill', refill_sum_series, non_refill_sum_series)
)

# 3. Calculate Historical Averages (The 'Baseline' for Mondays, Tuesdays, etc.)
# We group by the Name + Day + Hour to get the average for that specific item at that specific time
df['Avg_ERP'] = df.groupby(['ERP_NAME', 'day_name', 'hour_of_day'])['erp_QTY'].transform('mean')
df['Avg_Bundle'] = df.groupby(['BUNDLE_NAME', 'day_name', 'hour_of_day'])['IN_QTY'].transform('mean')
df['Avg_Site'] = df.groupby(['SITE_NAME', 'day_name', 'hour_of_day'])['SITE_QTY'].transform('mean')
df['Source_App'] = df.groupby(['INVOKEDBY', 'day_name', 'hour_of_day'])['INVOKEDBY_QTY'].transform('mean')

# 4. Calculate Ratios (Comparing Today vs. The Historical Average)
def calc_ratio(current, baseline):
    current_val = current.replace(0, np.nan)
    return ((current_val - baseline.fillna(0)).abs() / current_val).round(4)

df['Ratio_ERP'] = calc_ratio(df['erp_QTY'], df['Avg_ERP'])
df['Ratio_Other'] = calc_ratio(df['IN_QTY'], df['Avg_Bundle'])
df['Ratio_Site'] = calc_ratio(df['SITE_QTY'], df['Avg_Site'])
df['Ratio_Source'] = calc_ratio(df['INVOKEDBY_QTY'], df['Source_App'])

# 5. Global Sensory Ratio
avg_sum = df['Avg_ERP'] + df['Avg_Bundle'] + df['Avg_Site'] + df['Source_App']
qty_sum = df['erp_QTY'] + df['IN_QTY'] + df['SITE_QTY'] + df['INVOKEDBY_QTY']
df['Sensory_Ratio'] = ((avg_sum - qty_sum) / avg_sum.replace(0, np.nan)).abs().round(4)

df.fillna(0, inplace=True)

# ---------------- GLOBAL SIDEBAR FILTERS ----------------
st.sidebar.title("📑 Navigation")
# ADD THE NEW OPTION HERE:
page = st.sidebar.radio("Go to", ["Main Data Analysis", "Missing Sources Analysis", "Real-Time Alert Analysis", "📋 Logs Viewer"])

st.sidebar.divider()

# ============ AUTO/MANUAL RUN CONTROLS ============
st.sidebar.subheader("⚙️ Auto-Detection Controls")

col_auto_toggle, col_manual = st.sidebar.columns([2, 1])
with col_auto_toggle:
    st.session_state.auto_enabled = st.checkbox(
        "🤖 Auto Run (every 3 min)",
        value=st.session_state.get('auto_enabled', True),
        help="Enable/disable automatic detection every 3 minutes"
    )

with col_manual:
    if st.button("▶️ Run Now", use_container_width=True, key="manual_run_btn"):
        st.session_state.manual_run_active = True

# Show progress bar for manual run
if st.session_state.manual_run_active:
    progress_placeholder = st.sidebar.empty()
    status_placeholder = st.sidebar.empty()
    
    with progress_placeholder.container():
        progress_bar = st.progress(0)
    
    # Create progress callback function
    def update_progress(current, total, message):
        progress_bar.progress(min(current / total, 1.0))
        status_placeholder.info(f"⏳ {message}")
    
    # Run manual detection with progress
    try:
        status_placeholder.info("🚀 Starting manual run...")
        auto_detect_and_log_new_records(progress_callback=update_progress)
        progress_bar.progress(1.0)
        status_placeholder.success("✅ Manual run completed!")
        time.sleep(2)
        progress_placeholder.empty()
        status_placeholder.empty()
    except Exception as e:
        status_placeholder.error(f"❌ Error: {str(e)}")
        time.sleep(3)
        progress_placeholder.empty()
        status_placeholder.empty()
    
    st.session_state.manual_run_active = False

st.sidebar.divider()
st.sidebar.subheader("🌍 Global Filters")

yesterday = date.today() - timedelta(days=1)
selected_dates = st.sidebar.date_input("Date Range", value=(yesterday, yesterday))

selected_hours = st.sidebar.multiselect(
    "Select Hour(s)", 
    options=sorted(df["hour_of_day"].unique()), 
    default=sorted(df["hour_of_day"].unique())
)

# Apply global filters
if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    start_date = end_date = selected_dates

df_filtered = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
if selected_hours:
    df_filtered = df_filtered[df_filtered["hour_of_day"].isin(selected_hours)]


# ---------------- PAGE 1: MAIN ANALYSIS ----------------
if page == "Main Data Analysis":
    st.title("📊 Advanced Purchases analyzer")
    
    c1, c2 = st.columns(2)
    with c1:
        ratio_col = st.selectbox("Select Ratio Column", ["Ratio_ERP", "Ratio_Other", "Ratio_Site", "Ratio_Source", "Sensory_Ratio"])
    with c2:
        threshold = st.number_input("Ignore ratios below", min_value=0.0, max_value=1.0, value=0.05, step=0.01)

    # 1. Initial filter based on global sidebar and threshold
    df_display = df_filtered[df_filtered[ratio_col] >= threshold].copy()

    # 2. Setup the UI for text searching
    st.subheader("📋 Filtered Results")
    
    # --- NEW GLOBAL SEARCH FILTER ---
    search_query = st.text_input("🔍 Global Search (Show only rows containing):", placeholder="Type to search everything...", key="global_search")
    
    col_inc, col_exc = st.columns(2)
    with col_inc:
        include_text = st.text_input("➕ Include (comma separated):", placeholder="e.g. FastPay, FIB", key="inc_filter")
    with col_exc:
        exclude_text = st.text_input("➖ Exclude (comma separated):", placeholder="e.g. webService", key="exc_filter")

    # 3. APPLY MANUAL FILTERS
    def apply_manual_filter(dataframe, text, mode="include"):
        if not text:
            return dataframe
        terms = [t.strip() for t in text.split(",") if t.strip()]
        if mode == "include":
            final_mask = pd.Series(False, index=dataframe.index)
            for term in terms:
                term_mask = dataframe.astype(str).apply(lambda row: row.str.contains(term, case=False, na=False)).any(axis=1)
                final_mask = final_mask | term_mask
            return dataframe[final_mask]
        else:
            final_mask = pd.Series(True, index=dataframe.index)
            for term in terms:
                term_mask = ~dataframe.astype(str).apply(lambda row: row.str.contains(term, case=False, na=False)).any(axis=1)
                final_mask = final_mask & term_mask
            return dataframe[final_mask]

    # Apply the Global Search first (if text exists)
    if search_query:
        search_mask = df_display.astype(str).apply(lambda row: row.str.contains(search_query, case=False, na=False)).any(axis=1)
        df_display = df_display[search_mask]

    # Apply the existing Include/Exclude filters
    df_display = apply_manual_filter(df_display, include_text, mode="include")
    df_display = apply_manual_filter(df_display, exclude_text, mode="exclude")

    # 4. CALCULATE METRICS AFTER ALL FILTERS
    st.divider() # Optional: visual separation
    s1, s2, s3 = st.columns(3)
    s1.metric("📅 Range", f"{start_date}")
    # Now this sum and count will change as you type in the search boxes
    s2.metric("🔢 Total ERP Qty", f"{int(df_display['erp_count'].sum()):,}")
    s3.metric("🚨 Alert Rows", len(df_display))

    # 5. RENDER THE TABLE
    st.dataframe(
        df_display,
        use_container_width=True,
        column_config={
            "Ratio_ERP": st.column_config.ProgressColumn("Ratio ERP", min_value=0, max_value=1),
            "Ratio_Other": st.column_config.ProgressColumn("Ratio Other", min_value=0, max_value=1),
            "Ratio_Site": st.column_config.ProgressColumn("Ratio Site", min_value=0, max_value=1),
            "Ratio_Source": st.column_config.ProgressColumn("Ratio Source", min_value=0, max_value=1),
            "Sensory_Ratio": st.column_config.ProgressColumn("Sensory Ratio", min_value=0, max_value=1),
            "hour_start": st.column_config.DatetimeColumn("Hour Start", format="YYYY-MM-DD HH:mm"),
        }
    )
    # ---------------- PAGE 2: MISSING SOURCES ----------------

elif page == "Missing Sources Analysis":
    st.title("🔍 Missing Sales & Sources Analysis")

    # 1. Define the Master Lists from your requirements
    MASTER_LISTS = {
        "INVOKEDBY": [
            "webService", "Mediation/FastPay/FastPay_APP", "Mediation/FastPay/FastPay_POS",
            "WSC/FIB/bundle", "WSC/NCC_Balance/bundle", "Mediation/FastPay/FastPay_Kartat",
            "Mediation/FIB/FIB_APP", "no app", "CRM", 
            "Mediation/internal_showrooms/internal_showrooms", "API_Dina", "Dina_API",
            "Abdulla_Test", "API_mohammed"
        ],
        "ERP_NAME": [
            "F10K_50GB_FP_SPMIFI_45DAYS_100316", "F10K_50GB_WSC_SPMIFI_45DAYS_100317", "F15K_350GB_FP_SP_30DAYS_SULY_81",
            "F15K_350GB_WSC_SP_30DAYS_SULY_80", "F15K_60GB_WSC_SPMIFI_45DAY_NONFUP_100279", "F18K_350GB_FP_SP_30DAYS_84",
            "F18K_350GB_FP_SP_30DAYS_FMLY_56", "F18K_350GB_WSC_SP_30DAYS_85", "F18K_350GB_WSC_SP_30DAYS_FMLY_55",
            "F18K_640GB_FP_SP_30DAYS_SULY_72", "F18K_640GB_WSC_SP_30DAYS_SULY_73", "F2.5K_50GB_FP_SP_1DAY_100252",
            "F2.5K_50GB_WSC_SP_1DAY_100253", "F20K_640GB_FP_SP_30DAYS_78", "F20K_640GB_WSC_SP_30DAYS_79",
            "F30K_500GB_FP_SP_60DAYS_52", "F30K_500GB_WSC_SP_60DAYS_97", "F39K_1250GB_WSC_SP_30DAYS_76",
            "F39K_600GB_WSC_MR_30DAYS_70", "F4K_15GB_FP_SP_30DAYS_100196", "F5K_25GB_FP_SP_10DAYS_100251",
            "F5K_25GB_WSC_SP_10DAYS_100250", "F60K_2205GB_FP_SP_105DAYS_58", "F7K_20GB_FP_SP_30DAYS_100198",
            "F7K_20GB_WSC_SPMIFI_30DAY_100307", "F89K_1000GB_FP_SP_30DAYS_100176", "F9K_30GB_FP_SP_30DAYS_100199",
            "F9K_30GB_WSC_SPMIFI_30DAYS_100314", "N10K_30GB_FP_SPMIFI_45DAYS_100227", "N10K_30GB_WSC_SPMIFI_45DAYS_100226",
            "N12K_40GB_FP_SPMIFI_45DAYS_100321", "N12K_40GB_WSC_SPMIFI_45DAYS_100320", "N25K_350GB_FP_SPMIFI_30DAYS_D2GB_502",
            "N25K_350GB_FP_SPMIFI_30DAYS_FMLY_504", "N25K_350GB_WSC_SPMIFI_30DAYS_D2GB_501", "N25K_350GB_WSC_SPMIFI_30DAYS_FMLY_503",
            "N27K_750GB_FP_UB_30D_FUP_H310L350_100282", "N27K_750GB_WSC_UB_30D_FUP_FAMILY_100288", "N27K_750GB_WSC_UB_30D_FUPH310L350_100283",
            "N39K_1125GB_WSC_UB_45D_FUP_DLY3GB_100290", "N5K_50GB_FP_SPMIFI_5DAYS_100262", "N5K_50GB_WSC_SPMIFI_5DAYS_100267",
            "F39K_1250GB_FP_SP_30DAYS_75", "F69K_1200GB_WSC_MR_30DAYS_64", "F69K_2200GB_WSC_SP_30DAYS_67",
            "N20K_200GB_WSC_SPMIFI_30DAYS_100242", "N22K_200GB_WSC_SPMIFI_30DAYS_505", "N75K_1800GB_WSC_SPMIFI_120DAYS_100318",
            "80GBx12M_STAFF_SOG", "F27K_500GB_FP_SP_60DAYS_SULY_50", "N22K_200GB_FP_SPMIFI_30DAYS_506",
            "N29K_610GB_FP_SPMIFI_30DYS_100214_DLY3GB", "F27K_500GB_WSC_SP_60DAYS_SULY_94", "F39K_600GB_FP_MR_30DAYS_69",
            "F169K_2000GB_FP_SP_30DAYS_100177", "N27K_750GB_FP_UB_30D_FUP_FAMILY_100287", "N75K_1800GB_FP_SPMIFI_120DAYS_100319",
            "N39K_1125GB_FP_UB_45D_FUP_FAMILY_100294", "F120K_4725GB_WSC_SP_225DAYS_DLY6G_100328", "F60K_2205GB_WSC_SP_105DAYS_60",
            "N39K_1125GB_FP_UB_45D_FUP_DLY3GB_100286", "F69K_1200GB_FP_MR_30DAYS_63", "F69K_2200GB_FP_SP_30DAYS_66",
            "N29K_610GB_WSC_SPMFI_30DYS_100212_DLY3GB", "N39K_1125GB_WSC_UB_45D_FUP_FAMILY_100293", "Fastlink_Primary_offer_VIP",
            "Fastlink_Primary_offer_CRM", "10GBX12M_SOG", "95GBX12M_GP_SOG", "Corp_Special18K_Unlimited",
            "Fixed_LTE_Router_12M", "corporate_18k_fup_unlimited", "150GBx12M_STAFF_free_traffic",
            "Bugatti_Plus_FUP_12M_new_SP", "Site_Offer", "GW_Staff_60GB", "newroz_Primary_offer_CRM",
            "Fastlink_Primary_offer", "10gb_1d_app_gift", "bundle_10GB_1D_N4G_Comp", "FastPay_Gift",
            "10gb_1d_app_Comp", "F9K_80GB_FP_SPMIFI_30DAYS_SO_508", "F9K_80GB_WSC_SPMIFI_30DAYS_SO_507",
            "Zero_balance_bundle_test", "corp_unlimited_profile", "80GBx12M_Fastsim_Dealer",
            "1GBx12M_Monthly_Bundle_SOG", "20GBX12M_GP_SOG", "2GBX12M_GP_SOG", "30GBX12M_Hura_TV",
            "5GBX12M_GP_SOG", "95GBx12M_corporate", "Bugatti_Plus_FUP_12M_new_MiFi",
            "Ferrari_Plus_Unlimited_12M_SOG_SP", "kits_corp_130gb_12m", "newroz_home_35k_12m",
            "router_voip_12m", "F13.5K_200GB_WSC_SP_30DAYS_88", "compansate_20GB_13_Days",
            "Newroz_13Days_comp", "VIP_40GBx6M_SOG", "1GBx12M_FastPay_POS", "F18K_150GB_FP_SP_30DAYS_100230",
            "F6.75K_20GB_FIB_SPMIFI_30DAYS_100339"
        ],
        "BUNDLE_NAME": [
            "6_Months_Fastpay_FUP_SP", "Economic_22K_FUP_new_F", "Economic_25K_FUP_new_F", "Economic_25K_FUP_new_W",
            "Family_new_bundle_FP", "Family_New_Bundle_WS", "Newroz_4.5G_20K_WSC", "Vol_fastlink_18K_FUP_l3m_FP_high",
            "Vol_fastlink_18K_FUP_l3m_WSC_high", "Vol_Suly_15K_FUP_l3m_FP_high", "Vol_Suly_15K_FUP_l3m_WSC_high",
            "Vol_Two_Months_30K_high", "Vol_Two_Months_30K_l2m_ee_high", "Bundle_50GB_Suly_FP", "Bundle_50GB_Suly_WSC",
            "Suly_15K_FUP_l3m_FP_SP", "Suly_15K_FUP_l3m_WSC_SP", "porsche_new_nonfup_wsc", "fastlink_18K_FUP_l3m_FP_SP",
            "family_18K_FUP_l3m_ee_SP", "fastlink_18K_FUP_l3m_WSC_SP", "family_18K_FUP_l3m_WSC_SP", "Suly_18K_social_l4m_FP_SP",
            "Suly_18K_social_l4m_WSC_SP", "Daily_50GB_Fastpay_SP", "Daily_50GB_WSC_SP", "fastlink_20K_social_l4m_FP_SP",
            "fastlink_20K_social_l4m_WSC_SP", "Two_Months_30K_l2m_ee_SP", "Two_Months_30K_WSC_l2m_SP",
            "fastlink_39K_social_l4m_WSC_SP", "fastlink_39K_social_l4m_WSC_MR", "FTTH_Bundle_15GB_SP",
            "10_Days_NonFUP_Fastpay_25Gb_SP", "10_Days_NonFUP_WSC_SP", "fastlink_60K_social_l4m_ee_SP", "cml_20GB_Fastpay",
            "cml_20GB_WSC", "cml_30GB_Fastpay", "Package_9k_30GB_WSC", "Newroz_4G_PLUS_45Days_FP", "Newroz_4G_PLUS_45Days",
            "Newroz_12K_45Days_FP", "Newroz_12K_45Days_WSC", "Economic_25K_FUP_social_ee", "Family_25K_FUP_social_ee",
            "Economic_25K_FUP_social_WSC", "Family_25K_FUP_social_WSC", "newroz_fixed_lte_router_home_27k_FP",
            "newroz_fixed_lte_router_family_27k_WSC", "newroz_fixed_lte_router_home_27k_WSC", "newroz_fixed_lte_39K_router_WSC",
            "5_Days_N4G_Fastpay", "5_Days_N4G_W", "Vol_Two_Months_27K_Suly_ee_high", "fastlink_39K_social_l4m_FP_SP",
            "fastlink_69K_social_l4m_WSC_MR", "fastlink_69K_social_l4m_WSC_SP", "Economic_22K_FUP_social_WSC",
            "Bundle_3_months_plus_one_WSC", "Vol_family_18K_FUP_l3m_WSC_high", "80GBx12M_STAFF_SOG", "Vol_family_18K_FUP_l3m_ee_high",
            "Two_Months_27K_Suly_ee_l2m_SP", "Economic_22K_FUP_social_ee", "Newroz_4G_PLUS_Personal_F",
            "Two_Months_27K_Suly_WSC_l2m_SP", "fastlink_39K_social_l4m_FP_MR", "Vol_3_months_plus_one_WSC_high",
            "12_Months_Fastpay_FUP_SP", "newroz_fixed_lte_router_family_27k_FP", "Bundle_3_months_plus_one_FP",
            "newroz_family_lte_39K_router_Fastpay", "Bundle_6M_plus_45D_WSC_SP", "fastlink_60K_social_l4m_WSC_SP",
            "newroz_fixed_lte_39K_router_Fastpay", "Vol_Two_Months_30K_FIB_l2m_new_high", "fastlink_69K_social_l4m_FP_MR",
            "fastlink_69K_social_l4m_FP_SP", "Newroz_4G_PLUS_Personal", "Vol_3_months_plus_one_FP_high",
            "newroz_family_lte_39K_router_WSC", "Fastlink_Primary_offer", "10GBX12M_SOG", "95GBX12M_GP_SOG",
            "Vol_fastlink_18K_FUP_l3m_FP_high", "Vol_Corp_Special18K_Unlimited_high", "Fixed_LTE_Router_12M",
            "Vol_fastlink_39K_social_l4m_WSC_MR_high", "Vol_Economic_25K_FUP_social_ee_high", "corporate_18k_fup_unlimited",
            "150GBx12M_STAFF_free_traffic", "Bugatti_Plus_FUP_12M_new_SP", "Site_Offer", "Vol_fastlink_69K_social_l4m_FP_MR_high",
            "GW_Staff_60GB", "Vol_10gb_1d_app_gift", "Vol_bundle_10GB_1D_N4G_Comp", "Vol_FastPay_Gift",
            "Vol_fastlink_20K_social_l4m_FP_high", "Bundle_Student_9K_ee", "Bundle_Student_9K_WSC", "newroz_Primary_offer_CRM",
            "Vol_Zero_balance_bundle_test1", "Vol_80GBx12M_Fastsim_Dealer", "1GBx12M_Monthly_Bundle_SOG", "20GBX12M_GP_SOG",
            "2GBX12M_GP_SOG", "30GBX12M_Hura_TV", "5GBX12M_GP_SOG", "Vol_95GBx12M_corporate", "Bugatti_Plus_FUP_12M_new_MiFi",
            "Ferrari_Plus_Unlimited_12M_SOG_SP", "kits_corp_130gb_12m", "newroz_home_35k_12m", "router_voip_12m",
            "Vol_fastlink_18K_FUP_l3m_WSC_high", "Porsche_FUP_Chr_WSC_SP", "Vol_compansate_20GB", "Vol_Newroz_13Days_comp",
            "Vol_Bundle_6M_plus_45D_WSC_1GB_High", "Vol_Bundle_6M_plus_45D_WSC_daily", "Vol_Two_Months_30K_daily",
            "Vol_Two_Months_30K_l2m_ee_daily", "VIP_40GBx6M_SOG", "Vol_1GBx12M_FastPay_POS", "Porsche_FUP_FastPay_new_SP",
            "cml_20GB_FIB"
        ]
    }

    # 2. Timing Selection (Logic from your request)
    st.sidebar.subheader("🕒 Analysis Time")
    use_current = st.sidebar.checkbox("Use Last Hour (Auto)", value=True)

    # Determine date(s) and hour(s) to analyze based on user choice and global filters
    if use_current:
        target_time = datetime.now() - timedelta(hours=1)
        dates_to_check = [target_time.date()]
        hours_to_check = [target_time.hour]
    else:
        # use the global date range and selected hours
        dates_to_check = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        hours_to_check = selected_hours if selected_hours else sorted(df["hour_of_day"].unique())

    st.info(f"Analyzing Gaps for: **{dates_to_check[0]}{' to ' + str(dates_to_check[-1]) if len(dates_to_check)>1 else ''}** hours: **{hours_to_check}**")

    # Prepare history tracking for missing sources (single CSV history)
    history_file = 'missing_sources_history.csv'
    hist_df = load_history_df(history_file)
    seen_keys = set(hist_df['Key'].astype(str)) if not hist_df.empty else set()
    current_rows = []
    display_any = False

    # 3. Iterate dates/hours and check gaps (collect current rows)
    for chk_date in dates_to_check:
        for chk_hour in hours_to_check:
            analysis_df = df[(df['date'] == chk_date) & (df['hour_of_day'] == chk_hour)]
            header = f"Hour {chk_hour}:00 on {chk_date}"
            st.markdown(f"**{header}**")

            if analysis_df.empty:
                st.warning(f"No records for {header}.")
                continue

            for category, master_list in MASTER_LISTS.items():
                st.subheader(f"Checking: {category} — {header}")
                active_items = analysis_df[category].unique()
                missing = [item for item in master_list if item not in active_items]

                m_data = []
                for item in missing:
                    baseline_col = 'Source_App' if category == 'INVOKEDBY' else ('Avg_ERP' if category == 'ERP_NAME' else 'Avg_Bundle')
                    baseline = df[df[category] == item][baseline_col].max()
                    if baseline > 0:
                        key = f"{category}|{item}|{chk_date}|{chk_hour}"
                        row = {
                            'Key': key,
                            'Category': category,
                            'Item': item,
                            'HistoricalAvg': round(baseline, 2),
                            'Date': str(chk_date),
                            'Hour': int(chk_hour)
                        }
                        current_rows.append(row)
                        m_data.append(row)

                if m_data:
                    display_any = True
                    # show immediate results (mark NEW/OLD for display)
                    for r in m_data:
                        r_status = 'NEW' if r['Key'] not in seen_keys else 'OLD'
                        r_display = {k: v for k, v in r.items()}
                        r_display['Status'] = f"❌ MISSING ({r_status})"
                    st.warning(f"Found {len(m_data)} missing {category} entries with historical activity for {header}.")
                    st.dataframe(pd.DataFrame([{**{k:v for k,v in row.items()}, 'Status': ('NEW' if row['Key'] not in seen_keys else 'OLD')} for row in m_data]), use_container_width=True)
                else:
                    st.success(f"All critical {category} items are active (or have no historical baseline for {header}).")

    # Update history dataframe: mark current rows as NEW if not seen before, OLD if seen earlier
    now_ts = datetime.now().isoformat()
    now_dt = datetime.now()
    current_hour_key = f"{now_dt.strftime('%Y-%m-%d')}_{now_dt.hour}"
    current_keys = set(r['Key'] for r in current_rows)

    # Ensure hist_df has correct dtypes
    if hist_df.empty:
        hist_df = pd.DataFrame(columns=['Key','Category','Item','HistoricalAvg','Date','Hour','Status','FirstSeen','LastSeen','EntryHour'])

    # Index by Key for easy updates
    hist_df.set_index('Key', inplace=True, drop=False)

    # Add or update current entries
    for r in current_rows:
        k = r['Key']
        if k in hist_df.index:
            # Row exists - update LastSeen but keep Status as OLD (already seen before this run)
            hist_df.at[k, 'HistoricalAvg'] = r['HistoricalAvg']
            hist_df.at[k, 'LastSeen'] = now_ts
            if hist_df.at[k, 'Status'] != 'NEW':
                hist_df.at[k, 'Status'] = 'OLD'
        else:
            # Brand new row - mark as NEW
            hist_df.loc[k] = {
                'Key': k,
                'Category': r['Category'],
                'Item': r['Item'],
                'HistoricalAvg': r['HistoricalAvg'],
                'Date': r['Date'],
                'Hour': r['Hour'],
                'Status': 'NEW',
                'FirstSeen': now_ts,
                'LastSeen': now_ts,
                'EntryHour': current_hour_key
            }

    # Any previously NEW entries not in current_keys -> mark OLD (no longer appearing)
    for k, row in hist_df.iterrows():
        if row.get('Status') == 'NEW' and k not in current_keys:
            hist_df.at[k, 'Status'] = 'OLD'
            hist_df.at[k, 'LastSeen'] = now_ts

    # Save history
    save_history_df(history_file, hist_df.reset_index(drop=True))

    # Show combined history filtered by selected dates/hours
    hist_display = hist_df.reset_index(drop=True)
    try:
        hist_display['Hour'] = hist_display['Hour'].astype(int)
    except Exception:
        pass
    filtered_hist = hist_display[hist_display['Date'].isin([str(d) for d in dates_to_check]) & hist_display['Hour'].isin(hours_to_check)] if not hist_display.empty else hist_display
    if not filtered_hist.empty:
        st.subheader('Combined Missing Sources History (filtered)')
        st.dataframe(filtered_hist.sort_values(['Date','Hour','Category']), use_container_width=True)
    elif not display_any:
        st.info("No missing items with historical baseline found for the selected period.")
# ---------------- PAGE 3: REAL-TIME ALERT ANALYSIS ----------------
elif page == "Real-Time Alert Analysis":
    st.title("🚨 Last Hour Deviation Alerts")

    # Option to use global filters for multi-hour/date checks
    use_global = st.sidebar.checkbox("Use Global Date Range & Hours for Alerts", value=False)

    if use_global:
        dates_to_check = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        hours_to_check = selected_hours if selected_hours else sorted(df["hour_of_day"].unique())
    else:
        target_time = datetime.now() - timedelta(hours=1)
        dates_to_check = [target_time.date()]
        hours_to_check = [target_time.hour]

    history_alert_file = 'alerts_history.csv'
    hist_alert_df = load_history_df(history_alert_file)
    seen_alert_keys = set(hist_alert_df['Key'].astype(str)) if not hist_alert_df.empty else set()
    all_current_alert_keys = set()
    current_alert_rows = []

    def color_alerts(val):
        if val == "🚨 ALERT": return 'background-color: #ffcccc; color: black'
        if val == "✅ NORMAL": return 'background-color: #ccffcc; color: black'
        return ''

    for chk_date in dates_to_check:
        for chk_hour in hours_to_check:
            current_hour_df = df[(df['date'] == chk_date) & (df['hour_of_day'] == chk_hour)].copy()
            header = f"Alerts for {chk_hour}:00 on {chk_date}"
            st.markdown(f"**{header}**")

            if current_hour_df.empty:
                st.warning(f"⚠️ No data found for the hour: {chk_hour}:00 on {chk_date}")
                continue

            current_hour_df['ALERT_ERP'] = np.where(current_hour_df['erp_QTY'] > (current_hour_df['Avg_ERP'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            current_hour_df['ALERT_BUNDLE'] = np.where(current_hour_df['IN_QTY'] > (current_hour_df['Avg_Bundle'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            current_hour_df['ALERT_SITE'] = np.where(current_hour_df['SITE_QTY'] > (current_hour_df['Avg_Site'] * 1.2), "🚨 ALERT", "✅ NORMAL")
            current_hour_df['ALERT_SOURCE'] = np.where(current_hour_df['INVOKEDBY_QTY'] > (current_hour_df['Source_App'] * 1.2), "🚨 ALERT", "✅ NORMAL")

            display_cols = [
                'ERP_NAME', 'BUNDLE_NAME', 'SITE_NAME', 'INVOKEDBY', 'hour_start',
                'erp_QTY', 'IN_QTY', 'SITE_QTY', 'INVOKEDBY_QTY',
                'Avg_ERP', 'Avg_Bundle', 'Avg_Site', 'Source_App',
                'ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE'
            ]

            # collect alert rows/keys for history
            for _, row in current_hour_df.iterrows():
                for col in ['ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE']:
                    if row.get(col) == '🚨 ALERT':
                        key = f"ALERT|{col}|{row.get('ERP_NAME','') or ''}|{row.get('BUNDLE_NAME','') or ''}|{chk_date}|{chk_hour}"
                        all_current_alert_keys.add(key)
                        # Store full row data for audit trail
                        alert_row_data = {
                            'Key': key,
                            'Type': col,
                            'ERP_NAME': row.get('ERP_NAME',''),
                            'BUNDLE_NAME': row.get('BUNDLE_NAME',''),
                            'SITE_NAME': row.get('SITE_NAME',''),
                            'INVOKEDBY': row.get('INVOKEDBY',''),
                            'hour_start': row.get('hour_start',''),
                            'erp_QTY': row.get('erp_QTY',''),
                            'IN_QTY': row.get('IN_QTY',''),
                            'SITE_QTY': row.get('SITE_QTY',''),
                            'INVOKEDBY_QTY': row.get('INVOKEDBY_QTY',''),
                            'Avg_ERP': row.get('Avg_ERP',''),
                            'Avg_Bundle': row.get('Avg_Bundle',''),
                            'Avg_Site': row.get('Avg_Site',''),
                            'Source_App': row.get('Source_App',''),
                            'ALERT_ERP': row.get('ALERT_ERP',''),
                            'ALERT_BUNDLE': row.get('ALERT_BUNDLE',''),
                            'ALERT_SITE': row.get('ALERT_SITE',''),
                            'ALERT_SOURCE': row.get('ALERT_SOURCE',''),
                            'HistoricalAvg': '',
                            'Date': str(chk_date),
                            'Hour': int(chk_hour)
                        }
                        current_alert_rows.append(alert_row_data)

            total_qty = int(current_hour_df['erp_count'].sum())
            st.metric(label=f"Activity at {chk_hour}:00", value=f"{total_qty:,} Total Units")

            # display immediate annotated rows
            new_alerts = all_current_alert_keys - seen_alert_keys
            annotated = current_hour_df.copy()
            def row_status(row):
                parts = []
                for col in ['ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE']:
                    if row.get(col) == '🚨 ALERT':
                        k = f"ALERT|{col}|{row.get('ERP_NAME','') or ''}|{row.get('BUNDLE_NAME','') or ''}|{chk_date}|{chk_hour}"
                        parts.append('NEW' if k in new_alerts else 'OLD')
                return ','.join(parts) if parts else 'NORMAL'

            annotated['ALERT_STATUS'] = annotated.apply(row_status, axis=1)
            st.dataframe(annotated[display_cols + ['ALERT_STATUS']].style.applymap(color_alerts, subset=['ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE']), use_container_width=True)

    # Update alerts history: mark current as NEW if not seen before, OLD if seen in previous hours
    now_ts = datetime.now().isoformat()
    now_dt = datetime.now()
    current_hour_key = f"{now_dt.strftime('%Y-%m-%d')}_{now_dt.hour}"
    alert_hist = hist_alert_df
    if alert_hist.empty:
        alert_hist = pd.DataFrame(columns=['Key','Type','ERP_NAME','BUNDLE_NAME','SITE_NAME','INVOKEDBY','hour_start','erp_QTY','IN_QTY','SITE_QTY','INVOKEDBY_QTY','Avg_ERP','Avg_Bundle','Avg_Site','Source_App','ALERT_ERP','ALERT_BUNDLE','ALERT_SITE','ALERT_SOURCE','HistoricalAvg','Date','Hour','Status','FirstSeen','LastSeen','EntryHour','ALERT_STATUS'])
    alert_hist.set_index('Key', inplace=True, drop=False)
    
    for r in current_alert_rows:
        k = r['Key']
        if k in alert_hist.index:
            # Alert exists - update LastSeen but keep Status as OLD (already seen before this run)
            alert_hist.at[k, 'LastSeen'] = now_ts
            # Update all the data columns with latest values
            for col in ['SITE_NAME', 'INVOKEDBY', 'hour_start', 'erp_QTY', 'IN_QTY', 'SITE_QTY', 'INVOKEDBY_QTY', 'Avg_ERP', 'Avg_Bundle', 'Avg_Site', 'Source_App', 'ALERT_ERP', 'ALERT_BUNDLE', 'ALERT_SITE', 'ALERT_SOURCE']:
                if col in r:
                    alert_hist.at[k, col] = r[col]
            if alert_hist.at[k, 'Status'] != 'NEW':
                alert_hist.at[k, 'Status'] = 'OLD'
        else:
            # Brand new alert - mark as NEW
            alert_hist.loc[k] = {
                'Key': k,
                'Type': r['Type'],
                'ERP_NAME': r.get('ERP_NAME',''),
                'BUNDLE_NAME': r.get('BUNDLE_NAME',''),
                'SITE_NAME': r.get('SITE_NAME',''),
                'INVOKEDBY': r.get('INVOKEDBY',''),
                'hour_start': r.get('hour_start',''),
                'erp_QTY': r.get('erp_QTY',''),
                'IN_QTY': r.get('IN_QTY',''),
                'SITE_QTY': r.get('SITE_QTY',''),
                'INVOKEDBY_QTY': r.get('INVOKEDBY_QTY',''),
                'Avg_ERP': r.get('Avg_ERP',''),
                'Avg_Bundle': r.get('Avg_Bundle',''),
                'Avg_Site': r.get('Avg_Site',''),
                'Source_App': r.get('Source_App',''),
                'ALERT_ERP': r.get('ALERT_ERP',''),
                'ALERT_BUNDLE': r.get('ALERT_BUNDLE',''),
                'ALERT_SITE': r.get('ALERT_SITE',''),
                'ALERT_SOURCE': r.get('ALERT_SOURCE',''),
                'HistoricalAvg': r.get('HistoricalAvg',''),
                'Date': r.get('Date',''),
                'Hour': r.get('Hour',''),
                'Status': 'NEW',
                'FirstSeen': now_ts,
                'LastSeen': now_ts,
                'EntryHour': current_hour_key,
                'ALERT_STATUS': 'NEW'
            }

    # Any previously NEW alerts not in current_alert_keys -> mark OLD
    for k, row in alert_hist.iterrows():
        if row.get('Status') == 'NEW' and k not in all_current_alert_keys:
            alert_hist.at[k, 'Status'] = 'OLD'
            alert_hist.at[k, 'LastSeen'] = now_ts
        # Set ALERT_STATUS based on Status
        alert_hist.at[k, 'ALERT_STATUS'] = row.get('Status', 'OLD')

    save_history_df(history_alert_file, alert_hist.reset_index(drop=True))

    # show alerts history filtered by selected dates/hours
    hist_display = alert_hist.reset_index(drop=True)
    try:
        hist_display['Hour'] = hist_display['Hour'].astype(int)
    except Exception:
        pass
    filtered_alert_hist = hist_display[hist_display['Date'].isin([str(d) for d in dates_to_check]) & hist_display['Hour'].isin(hours_to_check)] if not hist_display.empty else hist_display
    if not filtered_alert_hist.empty:
        st.subheader('Combined Alerts History (filtered)')
        st.dataframe(filtered_alert_hist.sort_values(['Date','Hour','Type']), use_container_width=True)

# ============ PAGE 4: LOGS VIEWER ============
elif page == "📋 Logs Viewer":
    st.title("📋 Auto-Generated Logs Viewer")
    
    logs_dir = create_logs_directory()
    
    # Get all log files
    log_files = []
    if os.path.exists(logs_dir):
        for file in os.listdir(logs_dir):
            if file.endswith('.log'):
                log_files.append(file)
    
    log_files.sort(reverse=True)
    
    if not log_files:
        st.info("📭 No logs generated yet. Logs will be created automatically every hour.")
    else:
        st.success(f"✅ Found {len(log_files)} log files")
        
        # Filter by log type
        col1, col2 = st.columns(2)
        with col1:
            log_type = st.selectbox("Filter by Type", ["All", "Missing Sources", "Real-Time Alerts"])
        with col2:
            selected_date = st.date_input("Select Date (logs are organized by date)", value=date.today())
        
        # Filter and display
        filtered_files = []
        for f in log_files:
            if log_type == "Missing Sources" and "missing_sources" in f:
                filtered_files.append(f)
            elif log_type == "Real-Time Alerts" and "alerts" in f:
                filtered_files.append(f)
            elif log_type == "All":
                filtered_files.append(f)
        
        if not filtered_files:
            st.warning(f"⚠️ No {log_type} logs found for the selected period.")
        else:
            selected_file = st.selectbox("Select Log File", filtered_files)
            
            if selected_file:
                log_path = os.path.join(logs_dir, selected_file)
                try:
                    with open(log_path, 'r', encoding='utf-8') as f:
                        log_content = f.read()
                    
                    # Display log with code formatting
                    st.subheader(f"📄 {selected_file}")
                    st.info(f"Last Modified: {datetime.fromtimestamp(os.path.getmtime(log_path)).strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Display in expandable sections
                    sections = log_content.split('='*200)
                    for i, section in enumerate(sections[1:], 1):  # Skip first empty split
                        if section.strip():
                            with st.expander(f"📌 Entry {i}", expanded=(i==1)):
                                st.text(section.strip())
                    
                    # Download button
                    st.download_button(
                        label="📥 Download Log File",
                        data=log_content,
                        file_name=selected_file,
                        mime="text/plain"
                    )
                except Exception as e:
                    st.error(f"❌ Error reading log file: {e}")
        
        st.divider()
        st.subheader("📊 Logs Summary")
        
        # Summary stats
        col1, col2, col3 = st.columns(3)
        
        missing_sources_logs = [f for f in log_files if "missing_sources" in f]
        alerts_logs = [f for f in log_files if "alerts" in f]
        
        with col1:
            st.metric("📦 Missing Sources Logs", len(missing_sources_logs))
        with col2:
            st.metric("🚨 Alerts Logs", len(alerts_logs))
        with col3:
            st.metric("📁 Total Logs", len(log_files))
        
        # Show latest entries
        st.subheader("📅 Latest Logs Timeline")
        for log_file in log_files[:10]:  # Show last 10
            try:
                log_path = os.path.join(logs_dir, log_file)
                mod_time = datetime.fromtimestamp(os.path.getmtime(log_path))
                file_type = "📦 Missing Sources" if "missing_sources" in log_file else "🚨 Alerts"
                st.caption(f"{file_type} • {log_file} • {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception:
                pass