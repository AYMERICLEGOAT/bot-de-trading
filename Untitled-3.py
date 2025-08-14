# -*- coding: utf-8 -*-
################################################################################
#                   PROJECT KRONOS PROFIT - MONOLITH EDITION                   #
#        HYPER-PERFORMANCE CRYPTO TRADING BOT FRAMEWORK (OPTIMIZED)          #
#                        VERSION: 1.0 ALPHA PRIME                              #
#                                                                              #
#   *** EXTREME RISK & UNPROVEN COMPLEXITY - PURELY FOR EDUCATIONAL / ***    #
#   ***    CONCEPTUAL EXPLORATION & DEMONSTRATION OF ADVANCED CONCEPTS    ***  #
#   ***  USER ACCEPTS TOTAL & COMPLETE RESPONSIBILITY FOR ALL RISKS       ***  
#   ***  THERE IS NO GUARANTEE OF PROFIT - SUBSTANTIAL LOSSES ARE LIKELY  ***  
#   ***  DO NOT RUN WITH REAL MONEY WITHOUT EXTENSIVE VALIDATION,          *** 
#   ***  SIMULATION, AND A DEEP UNDERSTANDING OF EVERY COMPONENT.        ***  
#   ***  THIS CODE IS PROVIDED "AS IS", WITHOUT WARRANTIES OR CONDITIONS. ***  
################################################################################

# ==============================================================================
# SECTION 1: IMPORTS ET CONFIGURATION GLOBALE
# ==============================================================================

import asyncio
import websockets
import json
import os
import sys
from collections import deque, defaultdict
import pandas as pd
import numpy as np
import datetime
import time
import logging
import logging.handlers
import uuid
import re
import math
import argparse
import shutil
import copy
from typing import Dict, Any, List, Optional, Tuple, Union, Callable, Type
import functools
from dotenv import load_dotenv
# ML & Stats Libraries (REQUIRED - NO DUMMIES)
try:
    import ccxt.async_support as ccxt
    import lightgbm as lgb
    import joblib
    from sklearn.preprocessing import StandardScaler, MinMaxScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report, mean_squared_error, roc_auc_score
    from arch import arch_model
    import talib
    import requests
except ImportError as e:
    print(f"CRITICAL ERROR: Missing dependency. Please ensure all required libraries are installed (pip install -r requirements.txt). Details: {e}")
    sys.exit(1)

# ==============================================================================
# CONFIGURATION GLOBALE DU BOT
# ==============================================================================
load_dotenv()
CONFIG = {
    # --- SYSTEM SETTINGS ---
    "system": {
        "instance_name": "KRONOS_MONOLITH_V1",
        "main_loop_interval_ms": 100, 
        "startup_delay_seconds": 3,
        "logging_level": "INFO", 
        "log_file": "kronos_monolith.log",
        "log_max_bytes": 104857600, 
        "log_backup_count": 5,
        "system_heartbeat_interval_seconds": 30,
        "max_websocket_reconnect_attempts": 5,
        "websocket_reconnect_delay_seconds": 5,
        "metric_export_port": 8000, 
        "ntp_server": "pool.ntp.org", # Serveur NTP pour la synchronisation horaire
        "max_memory_usage_mb": 2048, # Limite mémoire (conceptuel, à surveiller)
        "cpu_usage_alert_threshold_pct": 80,
    },
    # --- EXCHANGE SETTINGS ---
    "exchange": {
        "name": "binanceusdm", 
        "sandbox_mode": True, 
        "default_symbol": "BTC/USDT",
        "active_symbols": ["BTC/USDT", "ETH/USDT", "BNB/USDT"], 
        "default_timeframes_for_ohlcv_reconstruction": ["1s", "5s", "10s", "1m", "5m", "15m", "1h", "4h", "1d"],
        "rest_api_timeout_seconds": 30,
        "websocket_uri_spot": "wss://stream.binance.com:9443",
        "websocket_uri_futures": "wss://fstream.binance.com", 
        "websocket_uri_futures_secondary": "wss://fstream.binance.com",
        "data_divergence_threshold_pct": 0.001,  # 0.1% de différence de prix déclenche une alerte
        "data_divergence_timeout_ms": 1000,      # L'alerte se déclenche si la divergence persiste pendant 1000ms (1 seconde)   
        "max_tick_data_deque_size": 500000, # Augmenté pour plus de données historiques de ticks
        "max_orderbook_depth_storage": 1000, # Nombre de niveaux de profondeur à stocker pour le carnet
        "max_ohlcv_history_for_features": 1500, # Augmenté
        "min_notional_multiplier": 1.05, 
        "commission_rates": { # Taux de commission par défaut (à vérifier sur l'échange)
            "spot": {"maker": 0.001, "taker": 0.001},
            "futures": {"maker": 0.0002, "taker": 0.0004}
        },
        "slippage_model": { # Paramètres pour l'estimation du slippage
            "type": "fixed_percentage", # "fixed_percentage", "orderbook_depth", "dynamic_volatility"
            "fixed_percentage_factor": 0.00005, # 0.005%
            "orderbook_depth_factor": 0.001, # Influence du volume sur le slippage
            "dynamic_volatility_multiplier": 0.1, # Multiplicateur de l'ATR pour le slippage
        },
        "market_info_refresh_interval_seconds": 3600, # Rafraîchissement des infos de marché (précisions, limites)
        "api_rate_limits_per_second": { # Limites de requêtes API (conceptuel, CCXT gère en partie)
            "rest_public": 10,
            "rest_private": 5,
            "websocket_subscriptions": 10,
        },
        "initial_sync_lookback_days": 7, # Jours de données historiques à charger au démarrage
    },
    # --- TRADING & RISK SETTINGS ---
    "trading": {
        "capital_currency": "USDT",
        "leverage": 20, 
        "leverage_overrides": { 
            "BTC/USDT": 25, "ETH/USDT": 20, "BNB/USDT": 15,
        },
        "default_entry_order_type": "LIMIT",
        "default_exit_order_type": "MARKET",
        "use_reduce_only": True,
        "max_concurrent_positions_total": 3, # Légèrement augmenté
        "max_concurrent_positions_per_symbol": 1,
        "max_exposure_per_symbol_pct_of_equity": 25.0, # Légèrement réduit
        "close_all_on_shutdown": False,
        "entry_order_time_in_force": "GTC",
        "exit_order_time_in_force": "GTC",
        "partial_fill_reentry_enabled": False, # Retenter si remplissage partiel pour entrée ?
        "position_consolidation_interval_seconds": 300, # Regrouper les petites positions en une (pour l'affichage)
    },
    "risk_management": {
        "global_max_drawdown_pct": 10.0, 
        "per_trade_max_loss_pct_of_capital": 0.5,
        "daily_profit_target_pct": 0.0, # Objectif de profit quotidien en %. Mettre à 0.0 pour désactiver.
        "fractional_kelly_ratio": 0.5,
        "stop_loss_atr_period": 14,
        "stop_loss_atr_multiplier": 2.5,
        "trailing_stop_atr_period": 12,
        "trailing_stop_atr_multiplier": 1.8,
        "trailing_stop_activation_rr": 1.0, 
        "take_profit_rr_ratio": 2.0, 
        "take_profit_levels": [ 
            {"pct": 30, "rr": 1.5},
            {"pct": 40, "rr": 2.5},
            {"pct": 30, "rr": 4.0},
        ],
        "move_sl_to_be_after_tp1": True,
        "be_buffer_pct": 0.05,
        "dynamic_risk_per_trade_min_pct": 0.2, 
        "dynamic_risk_per_trade_max_pct": 2.0,
        "garch_volatility_threshold_high": 0.02, 
        "garch_volatility_risk_multiplier_high": 0.8,
        "max_consecutive_losses_per_strategy": 5,
        "max_daily_drawdown_per_symbol_pct": 5.0,
        "kill_switch_action": "DISABLE_STRATEGY",
        "min_price_tick_distance": 0.0001, # Ajustement min pour SL/TP (ex: 0.01%)
        "liquidation_alert_threshold_pct": 90, # % de la marge où la liquidation est proche
        "funding_rate_threshold_long_pct": 0.0005, # > 0.05% pour alertes sur les funding rates
        "funding_rate_threshold_short_pct": -0.0005, # < -0.05% pour alertes
    },
    # --- EXECUTION ALGORITHM SETTINGS ---
    "execution": {
        "order_retry_attempts": 3,
        "order_retry_delay_seconds": 2,
        "order_fill_timeout_seconds": 15,
        "sniper_order_price_adjustment_factor": 0.0001,
        "sniper_max_reposition_attempts": 10,
        "sniper_reposition_delay_seconds": 0.1, 
        "vwap_slicer_num_slices": 5,
        "vwap_slicer_interval_seconds": 1,
        "websocket_reconciliation_interval_seconds": 5,
        "rest_reconciliation_interval_seconds": 60,
        "max_order_updates_per_cycle": 100,
        "cancel_order_retries": 3,
        "cancel_order_retry_delay_seconds": 1,
        "order_status_check_interval_seconds": 2, # Fréquence de vérification du statut d'un ordre
    },
    # --- RESEARCH & ML SETTINGS ---
    "research": {
        "historical_data_path": "./data/historical_kronos", 
        "backtest_initial_capital": 10000.0,
        "backtest_fees_pct": 0.0004, 
        "backtest_slippage_pct": 0.00005, 
        "volume_bar_size_usd": 50000,   # Une barre se forme tous les 50,000$ de volume
        "tick_bar_size": 150,           # Une barre se forme tous les 150 trades (ticks)
        "dollar_bar_size_usd": 75000,   # Une barre se forme tous les 75,000$ échangé
        "train_model_symbols": ["BTC/USDT"], 
        "train_model_timeframe_for_labels": "5m", 
        "train_model_future_price_target_minutes": 5,
        "train_model_price_change_threshold_pct": 0.1, 
        "garch_p_q_params": (1,1),
        "model_save_dir": "./models/",
        "feature_scaler_save_path": "./models/feature_scaler.joblib",
        "garch_model_save_path": "./models/garch_model.joblib",
        "ml_model_save_prefix": "kronos_alpha_model",
        "ml_model_confidence_threshold": 0.55, 
        "ml_features_to_use": [
            "OFI_100_tick", "OFI_1m_candle", "VWAP_Dev_5m", "Momentum_10m_slope", "Momentum_60m_slope",
            "Volatility_GARCH_Predicted_1h", "BB_Width_1m_Pct", "RSI_14_last", "MACD_Hist_last",
            "Price_Action_Pattern_Doji", # Exemple de feature de pattern
            "Orderbook_Imbalance_Top10_Ratio", # Exemple de feature de carnet d'ordres
        ],
        "walk_forward_validation_window_days": 90, 
        "walk_forward_test_window_days": 30,
        "model_validation_metric": "roc_auc", 
        "model_min_performance_threshold": 0.54,
        "optuna_trials_per_walk_forward": 50, 
        "optuna_objective_metric": "sharpe_ratio", 
        "feature_generation_intervals_seconds": { # Pour le calcul des features en backtest
            "OFI_100_tick": 1, # Chaque seconde
            "VWAP_Dev_5m": 60, # Chaque minute
            "Momentum_10m_slope": 60,
            "Volatility_GARCH_Predicted_1h": 3600, # Chaque heure
        }
    },
    # --- MONITORING & ALERTS SETTINGS ---
    "monitoring": {
        "enable_alerts": True,
        "alert_level_filter": "INFO", 
        "telegram_token": None, 
        "telegram_chat_id": None, 
        "discord_webhook_url": None, 
        "alert_rate_limit_global_count": 20, 
        "alert_rate_limit_global_period_sec": 60,
        "alert_templates": {
            "critical_error": "🔥 CRITICAL: {message} | Details: {details}",
            "position_opened": "🟢 OPENED {side} {symbol} @ {price:.4f} | Size: {amount:.4f} | Strategy: {strategy_id}",
            "position_closed": "🔴 CLOSED {side} {symbol} @ {price:.4f} | PnL: {pnl_usd:.2f} ({pnl_pct:.2f}%) | Reason: {reason}",
            "sl_hit": "🚨 STOP LOSS HIT for {symbol} {side} @ {price:.4f} | PnL: {pnl_usd:.2f} ({pnl_pct:.2f}%)",
            "tp_hit": "🎯 TAKE PROFIT HIT for {symbol} {side} @ {price:.4f} | PnL: {pnl_usd:.2f} ({pnl_pct:.2f}%)",
            "kill_switch_activated": "💀 GLOBAL KILL SWITCH ACTIVATED! Reason: {reason}",
            "warning": "⚠️ WARNING: {message}",
            "info": "ℹ️ INFO: {message}",
        },
    }
}

# --- Load API Keys from Environment Variables ---
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
# --- Update CONFIG with sensitive API keys (if found) ---
if BINANCE_API_KEY and BINANCE_API_SECRET:
    CONFIG["monitoring"]["telegram_token"] = os.environ.get("TELEGRAM_BOT_TOKEN")
    CONFIG["monitoring"]["telegram_chat_id"] = os.environ.get("TELEGRAM_CHAT_ID")
    CONFIG["monitoring"]["discord_webhook_url"] = os.environ.get("DISCORD_WEBHOOK_URL")

# --- Critical API Key Check ---
if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    print("CRITICAL ERROR: BINANCE_API_KEY or BINANCE_API_SECRET environment variables are not set.")
    print("Please set them before launching the bot (e.g., export BINANCE_API_KEY='your_key').")
    sys.exit(1)

# ==============================================================================
# ÉTAT PARTAGÉ GLOBAL ET VERROU (ACCÈS SÛR AUX DONNÉES EN TEMPS RÉEL)
# ==============================================================================

SHARED_STATE = {
    "is_running": False,
    "system_status": "INITIALIZING", # INITIALIZING, CONNECTING, LIVE_TRADING, PAUSED, ERROR, SHUTDOWN, HEALTH_WARNING
    "kill_switch_active": False,
    "last_loop_timestamp": 0.0,
    "loop_counter": 0,
    "current_utc_ms": 0, # Timestamp UTC courant en millisecondes pour synchronisation fine
    "market_regime": defaultdict(str), # NOUVEAU: Pour suivre le régime de marché (ex: "BULL_TREND")

    # --- Données Marché (Raw et Processed) ---
    "tick_data": defaultdict(lambda: deque(maxlen=CONFIG["exchange"]["max_tick_data_deque_size"])),
    "order_book_snapshots": defaultdict(lambda: {"bids": {}, "asks": {}, "last_update_id": 0, "timestamp": 0}), # {symbol: {bids: {price: qty}, asks: {price: qty}, ...}}
    "ohlcv_data": defaultdict(lambda: defaultdict(pd.DataFrame)), # {symbol: {timeframe: DataFrame}}
    "features_data": defaultdict(dict), # {symbol: {feature_name: value}}
    "market_info": {}, # {symbol: {precision: ..., limits: ..., active: ...}}
    "micro_bar_data": defaultdict(lambda: defaultdict(lambda: deque(maxlen=1000))), # NOUVEAU: Pour les barres de volume, tick, dollar

    # --- État du Portefeuille et Performance ---
    "global_capital": {
        "total": 0.0, "available": 0.0, "currency": CONFIG["trading"]["capital_currency"],
        "initial": 0.0, "peak": 0.0, "max_drawdown_pct": 0.0,
        "daily_pnl": 0.0, "daily_fees": 0.0, 
        "lifetime_realized_pnl": 0.0,
        "lifetime_fees_paid": 0.0,
        "daily_profit_reached": False, # NOUVEAU: Pour le suivi de l'objectif de profit quotidien
        "current_equity": 0.0, # NOUVEAU: Suivi de l'équité totale
    },
    "active_positions": {}, # {pos_id: position_dict}
    "open_orders": {}, # {order_id: order_dict} (ordres actifs sur l'échange, inclut CCXT order object)
    "client_order_map": {}, # {client_order_id: exchange_order_id} pour un suivi rapide
    "trade_history": deque(maxlen=5000),
    "strategy_performance_metrics": defaultdict(lambda: {
        "consecutive_losses": 0, "total_pnl_usd": 0.0, "daily_pnl_usd": 0.0,
        "last_reset_date": None,
        "win_rate": 0.0, "total_trades": 0, "profit_factor": 0.0,
        "sharpe_ratio_recent": 0.0,
        "alpha_decay_score": 0.0, # Indicateur de dégradation de l'alpha
        "current_status": "active", # active, paused, disabled
    }),
    "symbol_daily_drawdown": defaultdict(float),
    "live_strategy_parameters": {}, # {strategy_id: {param: value, enabled: True}}

    # --- Files d'Attente d'Événements (Communication Inter-Modules) ---
    "event_queue": None, 
    "alert_queue": asyncio.Queue(), 
    "tick_data_queue": defaultdict(asyncio.Queue),        # NOUVEAU
    "orderbook_data_queue": defaultdict(asyncio.Queue),   # NOUVEAU
    "kline_data_queue": defaultdict(asyncio.Queue),       # NOUVEAU
    
    "telemetry_data": defaultdict(dict), # Pour exposer les métriques (Prometheus/Grafana)
    "health_checks": { # État des composants pour le monitoring
        "websocket_status": defaultdict(str), # {stream_name: "connected" / "disconnected" / "reconnecting"}
        "api_status": "ok", # "ok", "degraded", "unavailable"
        "data_gaps_detected": defaultdict(bool), # {symbol_tf: True/False}
        "data_divergence_alerts": defaultdict(int), # NOUVEAU: Compteur d'alertes de divergence
        "last_heartbeat_sent": 0,
        "last_metrics_pushed": 0,
    },
}

STATE_LOCK = asyncio.Lock()

# ==============================================================================
# JOURNALISATION (LOGGING) AVANCÉE
# ==============================================================================

class CustomFormatter(logging.Formatter):
    """Formateur personnalisé pour ajouter des couleurs aux logs."""
    GREY = "\x1b[38;20m"
    BLUE = "\x1b[34;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    GREEN = "\x1b[32;20m"
    RESET = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: GREY + "%(asctime)s.%(msecs)03d - %(levelname)s - [%(threadName)s:%(process)d:%(thread)d] - %(filename)s:%(lineno)d - %(funcName)s - %(message)s" + RESET,
        logging.INFO: GREEN + "%(asctime)s.%(msecs)03d - %(levelname)s - [%(threadName)s:%(process)d:%(thread)d] - %(filename)s:%(lineno)d - %(funcName)s - %(message)s" + RESET,
        logging.WARNING: YELLOW + "%(asctime)s.%(msecs)03d - %(levelname)s - [%(threadName)s:%(process)d:%(thread)d] - %(filename)s:%(lineno)d - %(funcName)s - %(message)s" + RESET,
        logging.ERROR: RED + "%(asctime)s.%(msecs)03d - %(levelname)s - [%(threadName)s:%(process)d:%(thread)d] - %(filename)s:%(lineno)d - %(funcName)s - %(message)s" + RESET,
        logging.CRITICAL: BOLD_RED + "%(asctime)s.%(msecs)03d - %(levelname)s - [%(threadName)s:%(process)d:%(thread)d] - %(filename)s:%(lineno)d - %(funcName)s - %(message)s" + RESET
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)

class JsonFormatter(logging.Formatter):
    """Formateur JSON pour les logs structurés (utile pour les fichiers ou les systèmes de log centralisés)."""
    def format(self, record):
        log_record = {
            "timestamp": datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc).isoformat(timespec='milliseconds'),
            "level": record.levelname,
            "thread_name": record.threadName,
            "process_id": record.process,
            "thread_id": record.thread,
            "filename": record.filename,
            "lineno": record.lineno,
            "function": record.funcName,
            "message": record.getMessage(),
            "logger_name": record.name,
            # Ajouter des champs contextuels supplémentaires si nécessaires
        }
        if hasattr(record, 'extra_data'):
            log_record.update(record.extra_data) # Permet d'ajouter des données spécifiques à un log
        return json.dumps(log_record, ensure_ascii=False)

def setup_kronos_logging(config_level: str = "INFO", log_file: str = "kronos_monolith.log", max_bytes: int = 100 * 1024 * 1024, backup_count: int = 5):
    logger = logging.getLogger("KRONOS_MONOLITH")
    logger.setLevel(getattr(logging, config_level.upper(), logging.INFO))

    if not logger.handlers:
        # Console Handler (with colors)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(CustomFormatter())
        logger.addHandler(console_handler)

        # File Handler (plain text)
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d - %(levelname)s - [%(threadName)s:%(process)d:%(thread)d] - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"))
        logger.addHandler(file_handler)

        # JSON File Handler (for structured logs)
        json_log_file = log_file.replace(".log", "_structured.log")
        json_file_handler = logging.handlers.RotatingFileHandler(json_log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
        json_file_handler.setFormatter(JsonFormatter())
        logger.addHandler(json_file_handler)

    logger.info("=" * 80)
    logger.info(f"    KRONOS Monolith - Logging System Initialized")
    logger.info(f"    Log Level: {config_level.upper()}")
    logger.info(f"    Log File: {log_file}")
    logger.info("=" * 80)
    return logger

LOGGER = setup_kronos_logging(
    CONFIG["system"]["logging_level"],
    CONFIG["system"]["log_file"],
    CONFIG["system"]["log_max_bytes"],
    CONFIG["system"]["log_backup_count"]
)

# ==============================================================================
# SECTION 2: LE CŒUR ÉVÉNEMENTIEL (ASYNCIO)
# ==============================================================================

class Event:
    def __init__(self, event_type: str, data: Dict = None):
        self.type = event_type
        self.data = data if data is not None else {}
        self.timestamp = time.time()
        self.event_id = str(uuid.uuid4())

    def __repr__(self):
        return f"<Event Type: {self.type}, ID: {self.event_id[:8]}, Data: {self.data}>"

class MarketDataEvent(Event):
    def __init__(self, symbol: str, data_type: str, payload: Any):
        super().__init__("MARKET_DATA_UPDATE", {"symbol": symbol, "data_type": data_type, "payload": payload})
        self.symbol = symbol
        self.data_type = data_type # "tick", "ohlcv", "orderbook"
        self.payload = payload # tick: dict, ohlcv: pd.DataFrame, orderbook: dict

class FeatureEvent(Event):
    def __init__(self, symbol: str, features: Dict[str, Any], timestamp_ms: int):
        super().__init__("FEATURE_UPDATE", {"symbol": symbol, "features": features, "timestamp_ms": timestamp_ms})
        self.symbol = symbol
        self.features = features
        self.timestamp_ms = timestamp_ms

class SignalEvent(Event):
    def __init__(self, symbol: str, side: str, confidence: float, strategy_id: str, trigger_price: float, features_at_signal: Dict, signal_timestamp_ms: int):
        super().__init__("SIGNAL", {
            "symbol": symbol, "side": side, "confidence": confidence,
            "strategy_id": strategy_id, "trigger_price": trigger_price,
            "features_at_signal": features_at_signal, "signal_timestamp_ms": signal_timestamp_ms
        })
        self.symbol = symbol
        self.side = side
        self.confidence = confidence
        self.strategy_id = strategy_id
        self.trigger_price = trigger_price
        self.features_at_signal = features_at_signal
        self.signal_timestamp_ms = signal_timestamp_ms

class OrderRequestEvent(Event):
    def __init__(self, request_type: str, symbol: str, order_type: str, side: str, amount: float, price: Optional[float],
                 purpose: str, position_id: Optional[str] = None, client_order_id: Optional[str] = None, params: Optional[Dict] = None,
                 original_order_id: Optional[str] = None): # Pour les modifications/annulations
        super().__init__("ORDER_REQUEST", {
            "request_type": request_type, # "CREATE", "CANCEL", "MODIFY"
            "symbol": symbol, "order_type": order_type, "side": side, "amount": amount, "price": price,
            "purpose": purpose, "position_id": position_id, "client_order_id": client_order_id, "params": params,
            "original_order_id": original_order_id # L'ID de l'ordre à modifier/annuler
        })
        self.request_type = request_type
        self.symbol = symbol
        self.order_type = order_type
        self.side = side
        self.amount = amount
        self.price = price
        self.purpose = purpose
        self.position_id = position_id
        self.client_order_id = client_order_id or f"kronos_{uuid.uuid4().hex[:12]}"
        self.params = params if params is not None else {}
        self.original_order_id = original_order_id

class OrderUpdateEvent(Event):
    def __init__(self, exchange_order_id: str, client_order_id: str, symbol: str, status: str,
                 filled_amount: float, remaining_amount: float, fill_price: float,
                 purpose: str, position_id: str, fee: float, fee_currency: str,
                 order_details: Dict):
        super().__init__("ORDER_UPDATE", {
            "exchange_order_id": exchange_order_id, "client_order_id": client_order_id, "symbol": symbol,
            "status": status, # "open", "filled", "partial_fill", "canceled", "rejected", "expired"
            "filled_amount": filled_amount, "remaining_amount": remaining_amount, "fill_price": fill_price,
            "purpose": purpose, "position_id": position_id, "fee": fee, "fee_currency": fee_currency,
            "order_details": order_details # Contient l'objet CCXT raw order
        })
        self.exchange_order_id = exchange_order_id
        self.client_order_id = client_order_id
        self.symbol = symbol
        self.status = status
        self.filled_amount = filled_amount
        self.remaining_amount = remaining_amount
        self.fill_price = fill_price
        self.purpose = purpose
        self.position_id = position_id
        self.fee = fee
        self.fee_currency = fee_currency
        self.order_details = order_details

class PositionUpdateEvent(Event):
    def __init__(self, position_id: str, symbol: str, update_type: str, details: Dict):
        super().__init__("POSITION_UPDATE", {"position_id": position_id, "symbol": symbol, "update_type": update_type, "details": details})
        self.position_id = position_id
        self.symbol = symbol
        self.update_type = update_type # "OPENED", "SCALED_IN", "PARTIAL_CLOSE", "CLOSED", "SL_MOVED", "TSL_UPDATED", "LIQUIDATION_RISK"
        self.details = details

class HealthCheckEvent(Event):
    def __init__(self, component: str, status: str, message: str, level: str = "INFO"):
        super().__init__("HEALTH_CHECK", {"component": component, "status": status, "message": message, "level": level})
        self.component = component
        self.status = status # "ok", "warning", "critical"
        self.message = message
        self.level = level

class MetricUpdateEvent(Event):
    def __init__(self, metric_name: str, value: Any, labels: Dict = None):
        super().__init__("METRIC_UPDATE", {"metric_name": metric_name, "value": value, "labels": labels if labels else {}})
        self.metric_name = metric_name
        self.value = value
        self.labels = labels

class SystemAlertEvent(Event):
    def __init__(self, level: str, message: str, details: Dict = None):
        super().__init__("SYSTEM_ALERT", {"level": level, "message": message, "details": details if details else {}})
        self.level = level
        self.message = message
        self.details = details if details else {}

class BotCommandEvent(Event):
    def __init__(self, command: str, details: Dict = None):
        super().__init__("BOT_COMMAND", {"command": command, "details": details if details else {}})
        self.command = command
        self.details = details if details else {}

class RawMarketDataEvent(Event):
    def __init__(self, symbol: str, data_type: str, payload: Any, source_tag: str, sequence_id: Any):
        super().__init__("RAW_MARKET_DATA", {
            "symbol": symbol,
            "data_type": data_type,
            "payload": payload,
            "source_tag": source_tag,
            "sequence_id": sequence_id
        })
        self.symbol = symbol
        self.data_type = data_type
        self.payload = payload
        self.source_tag = source_tag
        self.sequence_id = sequence_id

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

# --- Basic Data Access & Type Conversion ---
def safe_get(data_dict: Dict, key_path: str, default: Any = None) -> Any:
    keys = key_path.split('.')
    val = data_dict
    try:
        for key in keys:
            if isinstance(val, dict):
                val = val[key]
            elif isinstance(val, list) and isinstance(key, str) and key.isdigit():
                val = val[int(key)]
            else:
                return default
        return val
    except (KeyError, TypeError, IndexError):
        return default

def to_float(value: Any, default: float = 0.0) -> float:
    try: return float(value)
    except (ValueError, TypeError): return default

def to_int(value: Any, default: int = 0) -> int:
    try: return int(value)
    except (ValueError, TypeError): return default

# --- Time & Timestamp Utilities ---
def get_current_utc_ms() -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

def convert_ms_to_datetime_utc(timestamp_ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)

def timeframe_to_seconds(timeframe_str: str) -> int:
    match = re.match(r'(\d+)([smhdwMy])', timeframe_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        if unit == 's': return value
        if unit == 'm': return value * 60
        if unit == 'h': return value * 3600
        if unit == 'd': return value * 86400
        if unit == 'w': return value * 86400 * 7
        if unit == 'M': return value * 86400 * 30 # Approximation
        if unit == 'y': return value * 86400 * 365 # Approximation
    LOGGER.warning(f"Invalid timeframe string: {timeframe_str}. Returning 0 seconds.")
    return 0

def generate_unique_id(prefix: str = "kronos") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

# --- Financial & Order Precision Utilities (Requires CCXT instance for market info) ---
def get_market_precision(exchange_instance: ccxt.Exchange, symbol: str) -> Tuple[Optional[int], Optional[int]]:
    market = exchange_instance.market(symbol)
    if not market or not market.get('precision'):
        LOGGER.warning(f"No precision info found for {symbol}. Using default decimal precision for amount/price.")
        return 8, 8 # Fallback to a high number of decimal places
    return market['precision'].get('amount'), market['precision'].get('price')

def get_market_limits(exchange_instance: ccxt.Exchange, symbol: str) -> Dict[str, Dict[str, Any]]:
    market = exchange_instance.market(symbol)
    if not market or not market.get('limits'):
        LOGGER.warning(f"No limits info found for {symbol}. Using empty limits.")
        return {'amount': {}, 'price': {}, 'cost': {}}
    return market['limits']

def adjust_amount_for_order(symbol: str, amount: float, exchange_instance: ccxt.Exchange) -> float:
    if amount is None: return None
    market = exchange_instance.market(symbol)
    if not market or not market.get('precision', {}).get('amount'):
        LOGGER.warning(f"Precision info not found for {symbol}. Skipping amount adjustment.")
        return amount
    try:
        # ccxt.decimal_to_precision handles both amount and price precision modes
        # For amount, precision mode is typically TICK_SIZE or DECIMAL_PLACES
        return float(exchange_instance.amount_to_precision(amount, market['precision']['amount']))
    except Exception as e:
        LOGGER.error(f"Error adjusting amount {amount} for {symbol}: {e}. Returning original amount.")
        return amount

def adjust_price_for_order(symbol: str, price: float, exchange_instance: ccxt.Exchange) -> float:
    if price is None: return None
    market = exchange_instance.market(symbol)
    if not market or not market.get('precision', {}).get('price'):
        LOGGER.warning(f"Precision info not found for {symbol}. Skipping price adjustment.")
        return price
    try:
        # For price, precision mode is typically TICK_SIZE or DECIMAL_PLACES
        return float(exchange_instance.price_to_precision(price, market['precision']['price']))
    except Exception as e:
        LOGGER.error(f"Error adjusting price {price} for {symbol}: {e}. Returning original price.")
        return price

def adjust_price_for_order_conservative(symbol: str, price: float, order_side: str, is_stop_loss: bool, exchange_instance: ccxt.Exchange) -> float:
    if price is None: return None
    market = exchange_instance.market(symbol)
    if not market or not market.get('precision', {}).get('price'):
        LOGGER.warning(f"Precision info not found for {symbol}. Using default precision 8 for price (conservative).")
        return float(f"{price:.8f}")

    price_prec = market['precision']['price']
    
    # Use exchange's precision mode (e.g., TRUNCATE, ROUND, ROUND_UP, ROUND_DOWN)
    # And counting mode (DECIMAL_PLACES, SIGNIFICANT_DIGITS, TICK_SIZE)
    # This requires `exchange.decimal_to_precision` which is more explicit.
    
    # Default to ROUND for now if direct access to exchange.decimal_to_precision or precisionMode is complex
    # For conservative rounding:
    # SL for BUY (SELL order) -> round down (lower SL price)
    # SL for SELL (BUY order) -> round up (higher SL price)
    # TP for BUY (SELL order) -> round up (higher TP price for short, closer to entry)
    # TP for SELL (BUY order) -> round down (lower TP price for long, closer to entry)

    if is_stop_loss:
        if order_side.upper() == 'SELL': # BUY position SL is a SELL order (price below entry) -> round down
            rounding_mode = ccxt.ROUND_DOWN
        else: # SELL position SL is a BUY order (price above entry) -> round up
            rounding_mode = ccxt.ROUND_UP
    else: # Take Profit
        if order_side.upper() == 'SELL': # BUY position TP is a SELL order (price above entry) -> round up (away from entry for long)
            rounding_mode = ccxt.ROUND_UP # Or ROUND_FLOOR if closer to market desired
        else: # SELL position TP is a BUY order (price below entry) -> round down (away from entry for short)
            rounding_mode = ccxt.ROUND_DOWN # Or ROUND_CEIL if closer to market desired

    try:
        # Use decimal_to_precision for explicit rounding control
        adjusted_price_str = exchange_instance.decimal_to_precision(
            price,
            rounding_mode=rounding_mode,
            counting_mode=exchange_instance.options.get('precisionMode', ccxt.DECIMAL_PLACES), # Default if not set
            precision=price_prec
        )
        return float(adjusted_price_str)
    except Exception as e:
        LOGGER.error(f"Error adjusting price conservatively {price} for {symbol}: {e}. Returning original price.")
        return price

# --- Portfolio & Performance Metrics Calculation ---
def calculate_pnl_usd(entry_price: float, current_price: float, amount: float, side: str) -> float:
    if side.upper() == 'BUY': return (current_price - entry_price) * amount
    if side.upper() == 'SELL': return (entry_price - current_price) * amount
    return 0.0

def calculate_pnl_pct(pnl_usd: float, initial_margin_usd: float) -> float:
    if initial_margin_usd > 1e-9: return (pnl_usd / initial_margin_usd) * 100
    return 0.0

def calculate_kelly_fraction(probability_of_win: float, win_loss_ratio: float, kelly_fractional_factor: float = 1.0) -> float:
    """Calculates optimal position size fraction using Kelly Criterion."""
    if win_loss_ratio <= 0: return 0.0
    if not (0 <= probability_of_win <= 1): return 0.0

    # Kelly Formula: f = p - (1-p)/b
    # f = fraction of capital to bet
    # p = probability of winning (confidence)
    # b = win/loss ratio (R:R)
    optimal_fraction = probability_of_win - ((1 - probability_of_win) / win_loss_ratio)
    
    # Apply fractional Kelly (e.g., Half-Kelly)
    return max(0.0, optimal_fraction * kelly_fractional_factor)

def estimate_slippage_from_order_book(order_book: Dict, order_side: str, amount: float, current_price: float, slippage_model_config: Dict) -> float:
    if slippage_model_config["type"] == "fixed_percentage":
        return current_price * slippage_model_config["fixed_percentage_factor"]

    # This is a conceptual implementation for "orderbook_depth"
    # A real implementation would iterate through order book levels
    # to find the weighted average fill price for the given amount.
    estimated_slippage = 0.0
    if order_side.upper() == "BUY":
        depth_levels = sorted(order_book.get('asks', {}).items(), key=lambda x: float(x[0]))
    else:
        depth_levels = sorted(order_book.get('bids', {}).items(), key=lambda x: float(x[0]), reverse=True)

    remaining_amount = amount
    total_cost_or_revenue = 0.0
    total_filled_qty = 0.0

    for price_str, qty_str in depth_levels:
        price = float(price_str)
        qty = float(qty_str)
        if remaining_amount <= 0: break

        fill_qty = min(remaining_amount, qty)
        total_cost_or_revenue += fill_qty * price
        total_filled_qty += fill_qty
        remaining_amount -= fill_qty
    
    if total_filled_qty > 0:
        avg_fill_price_from_ob = total_cost_or_revenue / total_filled_qty
        estimated_slippage = abs(avg_fill_price_from_ob - current_price)
    
    return estimated_slippage * slippage_model_config.get("orderbook_depth_factor", 1.0) # Apply adjustment factor

def calculate_calmar_ratio(total_return_pct: float, max_drawdown_pct: float, periods_per_year: int = 1) -> float:
    if max_drawdown_pct <= 0: return float('inf') # Drawdown 0 means infinite ratio if positive return
    # Annualized return / Max Drawdown (expressed as positive value)
    return (total_return_pct / periods_per_year) / max_drawdown_pct

# --- Data Validation & Health Checks ---
def validate_ohlcv_dataframe(df: pd.DataFrame, timeframe_seconds: int) -> bool:
    if df.empty:
        LOGGER.warning("OHLCV DataFrame is empty.")
        return False
    if not all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume']):
        LOGGER.warning("OHLCV DataFrame missing essential columns (open, high, low, close, volume).")
        return False
    if not isinstance(df.index, pd.DatetimeIndex):
        LOGGER.warning("OHLCV DataFrame index is not a DatetimeIndex.")
        return False
    if df.index.has_duplicates:
        LOGGER.warning("OHLCV DataFrame contains duplicate index entries.")
        return False
    
    # Check for chronological order
    if not df.index.is_monotonic_increasing:
        LOGGER.warning("OHLCV DataFrame index is not monotonically increasing.")
        return False

    # Check for NaNs in critical columns (after forward/backward fill in DataHandler)
    if df[['open', 'high', 'low', 'close', 'volume']].isnull().any().any():
        LOGGER.warning("OHLCV DataFrame contains NaN values in critical columns.")
        return False

    # Check for consistent time intervals (approximate due to exchange data issues)
    if len(df) > 1:
        diffs = df.index.to_series().diff().dropna().dt.total_seconds()
        if not diffs.empty:
            # Allow for slight variations or missing candles in a small percentage
            tolerance = timeframe_seconds * 0.1 # 10% tolerance
            if not ((diffs >= timeframe_seconds - tolerance) & (diffs <= timeframe_seconds + tolerance)).all():
                 # Check if the majority of gaps are small
                 if (diffs > (timeframe_seconds + tolerance)).mean() > 0.05: # More than 5% large gaps
                    LOGGER.warning(f"OHLCV DataFrame has inconsistent time intervals. Expected ~{timeframe_seconds}s, actual median: {diffs.median():.1f}s, max: {diffs.max():.1f}s.")
                    # Can still return True if inconsistencies are not too severe
            
    return True

# --- Decorators for Robustness ---
def retry_async(max_retries: int, delay_seconds: float, catch_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception):
    """Décorateur pour retenter une fonction async en cas d'erreur."""
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for i in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except catch_exceptions as e:
                    LOGGER.warning(f"Attempt {i+1}/{max_retries+1} for {func.__name__} failed: {e}")
                    if i < max_retries:
                        await asyncio.sleep(delay_seconds * (i + 1)) # Exponential backoff
                    else:
                        LOGGER.error(f"Max retries reached for {func.__name__}. Giving up.")
                        raise
                except asyncio.CancelledError:
                    LOGGER.warning(f"Task {func.__name__} was cancelled during retry.")
                    raise
        return wrapper
    return decorator

class CircuitBreaker:
    """Implémente un disjoncteur pour protéger contre les services défaillants."""
    def __init__(self, failure_threshold: int, recovery_timeout: int, logger: logging.Logger):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.logger = logger
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED" # CLOSED, OPEN, HALF-OPEN

    def __call__(self, func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "HALF-OPEN"
                    self.logger.info(f"Circuit Breaker for {func.__name__} is now HALF-OPEN. Trying a call.")
                else:
                    self.logger.warning(f"Circuit Breaker for {func.__name__} is OPEN. Skipping call.")
                    raise CircuitBreakerOpenError(f"Circuit breaker for {func.__name__} is open.")
            
            try:
                result = await func(*args, **kwargs)
                if self.state == "HALF-OPEN":
                    self.logger.info(f"Circuit Breaker for {func.__name__} is now CLOSED (successful call).")
                    self.state = "CLOSED"
                    self.failures = 0
                return result
            except Exception as e:
                self.failures += 1
                self.last_failure_time = time.time()
                self.logger.error(f"Circuit Breaker: Call to {func.__name__} failed. Failures: {self.failures}/{self.failure_threshold}. Error: {e}")
                if self.failures >= self.failure_threshold:
                    self.state = "OPEN"
                    self.logger.critical(f"Circuit Breaker for {func.__name__} is now OPEN. Too many failures.")
                raise # Re-raise the original exception
        return wrapper

class CircuitBreakerOpenError(Exception):
    """Exception levée quand le disjoncteur est ouvert."""
    pass
def calculate_sharpe_ratio(returns: pd.Series, periods_per_year: int = 252, risk_free_rate: float = 0.0) -> float:
    """Calcule le ratio de Sharpe annualisé à partir d'une série de rendements."""
    if returns.std() == 0: return 0.0
    excess_returns = returns - (risk_free_rate / periods_per_year)
    sharpe = (excess_returns.mean() / excess_returns.std()) * math.sqrt(periods_per_year)
    return sharpe

def calculate_drawdown(equity_curve: pd.Series) -> Tuple[float, pd.Series]:
    """Calcule le drawdown maximum et la série de drawdowns."""
    if equity_curve.empty: return 0.0, pd.Series()
    cumulative_max = equity_curve.cummax()
    drawdown = (equity_curve - cumulative_max) / cumulative_max
    max_drawdown_pct = -drawdown.min() * 100 if not drawdown.empty else 0.0
    return max_drawdown_pct, drawdown

# PARTIE 2/5 : LES MODULES SPÉCIALISÉS (DATA, ALPHA, RISK) - APPROFONDISSEMENT

# ==============================================================================
# SECTION 3: LES MODULES SPÉCIALISÉS (CLASSES ASYNC) - REFONTE TOTALE
# ==============================================================================

# --- NEW: MarketDataGateway (replaces DataHandler's raw data collection role) ---
class MarketDataGateway:
    """
    MarketDataGateway: The perfect auditory system of the bot. It hears everything, instantly,
    without judging or interpreting. It manages redundant data sources, performs cross-validation,
    and diffuses data through specialized, channeled queues for minimal latency.
    """
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_config = config["exchange"]
        self.active_symbols = self.exchange_config["active_symbols"]
        self.ws_uri_primary = self.exchange_config.get("websocket_uri_futures") # Primary source (e.g., Binance)
        self.ws_uri_secondary = self.exchange_config.get("websocket_uri_futures_secondary") # Secondary source (OPTIONAL)
        self.data_divergence_threshold_pct = self.exchange_config["data_divergence_threshold_pct"]
        self.data_divergence_timeout_ms = self.exchange_config["data_divergence_timeout_ms"]

        self._running = False
        self.ws_connections: Dict[str, asyncio.Task] = {} # {connection_name: task}
        self.last_ws_message_time_ns = defaultdict(int) # {connection_name: time.perf_counter_ns()} for raw latency

        # Queues for channeled diffusion of raw market data to downstream modules (e.g., FeatureEngine)
        self.tick_queues: Dict[str, asyncio.Queue] = SHARED_STATE["tick_data_queue"] # [Refonte: Canalisation]
        self.orderbook_queues: Dict[str, asyncio.Queue] = SHARED_STATE["orderbook_data_queue"] # [Refonte: Canalisation]
        self.kline_queues: Dict[str, asyncio.Queue] = SHARED_STATE["kline_data_queue"] # [Refonte: Canalisation]

        # For cross-validation: store last received data from primary/secondary sources
        # Key: "symbol_data_type" (e.g., "BTC/USDT_trade_tick")
        self.last_primary_data: Dict[str, Dict[str, Any]] = defaultdict(dict) # {payload, timestamp_ns, seq_id}
        self.last_secondary_data: Dict[str, Dict[str, Any]] = defaultdict(dict) # {payload, timestamp_ns, seq_id}
        self.divergence_start_time_ns = defaultdict(int) # {symbol_stream_type: timestamp_ns of first divergence}

        LOGGER.info("MarketDataGateway initialized. Ready for dual-source data acquisition.")

    async def run(self):
        """Starts the MarketDataGateway by establishing dual WebSocket connections and integrity monitoring."""
        self._running = True
        LOGGER.info("MarketDataGateway starting. Establishing dual-source WebSocket connections for active symbols.")
        
        streams = []
        for sym in self.active_symbols:
            # Construct stream names based on Binance futures conventions (adjust for other exchanges)
            clean_sym = sym.lower().replace('/', '')
            streams.append(f"{clean_sym}@trade")
            streams.append(f"{clean_sym}@depth@100ms") 
            # Add kline stream if MarketDataGateway is expected to receive them directly from source,
            # otherwise, FeatureEngine will reconstruct.
            # streams.append(f"{clean_sym}@kline_1m") 

        # Combined stream URI for primary exchange
        primary_combined_stream_uri = f"{self.ws_uri_primary}/stream?streams={'/'.join(streams)}"
        # Combined stream URI for secondary exchange (could be different endpoint, or same but for redundancy)
        secondary_combined_stream_uri = f"{self.ws_uri_secondary}/stream?streams={'/'.join(streams)}" 

         # Launch WebSocket connection tasks
        if self.ws_uri_primary:
            primary_combined_stream_uri = f"{self.ws_uri_primary}/stream?streams={'/'.join(streams)}"
            self.ws_connections["primary_source"] = asyncio.create_task(
                self._connect_websocket(primary_combined_stream_uri, source_tag="primary"),
                name="MDG_WS_Primary"
            )
        else:
            LOGGER.critical("MDG: websocket_uri_futures is not defined in config! Cannot run.")
            return # Stop if primary source is missing

        if self.ws_uri_secondary:
            secondary_combined_stream_uri = f"{self.ws_uri_secondary}/stream?streams={'/'.join(streams)}" 
            self.ws_connections["secondary_source"] = asyncio.create_task(
                self._connect_websocket(secondary_combined_stream_uri, source_tag="secondary"),
                name="MDG_WS_Secondary"
            )
            LOGGER.info("MDG: Secondary WebSocket source is configured.")
        else:
            LOGGER.warning("MDG: Secondary WebSocket source is not configured. Running in single-source mode (less resilient).")

    async def _connect_websocket(self, uri: str, source_tag: str):
        """
        Establishes and maintains a WebSocket connection to a data source.
        Implements reconnection logic and tracks connection status.
        [Refonte: Robustesse, redondance]
        """
        reconnect_attempts = 0
        connection_name = f"{source_tag}_{uri.split('/')[2].replace('.', '_')}" # e.g., "primary_fstream_binance_com"
        
        while self._running and reconnect_attempts < CONFIG["system"]["max_websocket_reconnect_attempts"]:
            try:
                LOGGER.info(f"MDG: Attempting to connect to WS ({source_tag}): {uri}")
                # Use ping/pong for keep-alive
                async with websockets.connect(uri, ping_interval=30, ping_timeout=20) as ws:
                    LOGGER.info(f"MDG: Successfully connected to WS ({source_tag}): {uri}")
                    reconnect_attempts = 0 # Reset attempts on successful connection
                    async with STATE_LOCK:
                        SHARED_STATE["health_checks"]["websocket_status"][connection_name] = "connected"
                    
                    while self._running:
                        try:
                            # Wait for a message with a timeout to detect idle connections
                            message = await asyncio.wait_for(ws.recv(), timeout=60)
                            self.last_ws_message_time_ns[connection_name] = time.perf_counter_ns() # High precision timestamp for latency
                            await self._process_raw_ws_message(message, source_tag)
                            async with STATE_LOCK:
                                SHARED_STATE["telemetry_data"][f"websocket_messages_received_{source_tag}"] = \
                                    SHARED_STATE["telemetry_data"].get(f"websocket_messages_received_{source_tag}", 0) + 1
                        except asyncio.TimeoutError:
                            LOGGER.debug(f"MDG: WS ({source_tag}) {uri} is idle, sending ping to keep connection alive.")
                            await ws.ping() # Explicitly send ping if no data for timeout period
                        except websockets.exceptions.ConnectionClosedOK:
                            LOGGER.warning(f"MDG: WebSocket ({source_tag}) {uri} closed normally.")
                            break # Exit inner loop, trigger reconnect if _running is still True
                        except websockets.exceptions.ConnectionClosedError as e:
                            LOGGER.error(f"MDG: WebSocket ({source_tag}) {uri} closed with error: {e}. Attempting reconnect.", exc_info=True)
                            break # Exit inner loop, trigger reconnect
                        except Exception as e:
                            LOGGER.error(f"MDG: Unhandled error processing WS message from ({source_tag}) {uri}: {e}. Connection lost.", exc_info=True)
                            break # Exit inner loop, trigger reconnect
            except websockets.exceptions.InvalidURI as e:
                LOGGER.critical(f"MDG: Invalid WebSocket URI for {source_tag}: {uri}. This is a configuration error. Aborting connection.", exc_info=True)
                self._running = False # Critical configuration error, stop gateway
            except Exception as e:
                LOGGER.error(f"MDG: Failed to establish WS connection ({source_tag}) to {uri} (Attempt {reconnect_attempts+1}/{CONFIG['system']['max_websocket_reconnect_attempts']}): {e}", exc_info=True)
                reconnect_attempts += 1
                async with STATE_LOCK:
                    SHARED_STATE["health_checks"]["websocket_status"][connection_name] = "reconnecting"
                await asyncio.sleep(CONFIG["system"]["websocket_reconnect_delay_seconds"] * (reconnect_attempts ** 1.5)) # Exponential backoff
        
        if self._running and reconnect_attempts >= CONFIG["system"]["max_websocket_reconnect_attempts"]:
            LOGGER.critical(f"MDG: Failed to connect to WS ({source_tag}) {uri} after {CONFIG['system']['max_websocket_reconnect_attempts']} attempts. Aborting MarketDataGateway operation due to persistent connection failure.")
            await SHARED_STATE["alert_queue"].put(HealthCheckEvent("MarketDataGateway", "critical", f"MDG: WS connection to {uri} ({source_tag}) failed permanently. Major market data loss!", level="CRITICAL"))
            async with STATE_LOCK:
                SHARED_STATE["health_checks"]["websocket_status"][connection_name] = "disconnected_permanent"
            self._running = False # Propagate critical failure to stop the entire gateway

    async def _process_raw_ws_message(self, message: str, source_tag: str):
        """
        Parses raw WebSocket messages, extracts relevant data, and pushes
        RawMarketDataEvents to specialized, channeled queues for downstream modules.
        [Refonte: Parsage rapide, canalisation]
        """
        try:
            # Use ujson for faster JSON decoding
            data = json.loads(message)
            
            # Binance combined stream wraps data in a 'data' field
            if 'data' in data and 'stream' in data:
                stream_name = data['stream']
                event_data = data['data']
                
                symbol_raw = event_data.get('s') # e.g., 'BTCUSDT'
                symbol = symbol_raw.replace('USDT', '/USDT') if symbol_raw else None # Normalize to CCXT format
                event_type = event_data.get('e') # e.g., 'trade', 'depthUpdate', 'kline'

                if symbol not in self.active_symbols or symbol is None: 
                    LOGGER.debug(f"MDG: Received data for inactive/invalid symbol {symbol_raw} (source: {source_tag}). Ignoring.")
                    return 

                current_timestamp_ns = time.perf_counter_ns() # Capture time of receipt with high precision

                if event_type == 'trade':
                    trade_id = event_data.get('t') # Unique trade ID for cross-validation
                    await self.tick_queues[symbol].put(RawMarketDataEvent(symbol, "trade_tick", event_data, source_tag, trade_id))
                    self._update_last_received_data(symbol, "trade_tick", event_data, source_tag, trade_id, current_timestamp_ns)
                    LOGGER.debug(f"MDG: {source_tag} -> Tick for {symbol} pushed (ID: {trade_id}).")
                elif event_type == 'depthUpdate':
                    last_update_id = event_data.get('u') # Binance's lastUpdateId for depth
                    await self.orderbook_queues[symbol].put(RawMarketDataEvent(symbol, "depth_update", event_data, source_tag, last_update_id))
                    self._update_last_received_data(symbol, "depth_update", event_data, source_tag, last_update_id, current_timestamp_ns)
                    LOGGER.debug(f"MDG: {source_tag} -> Depth update for {symbol} pushed (Last ID: {last_update_id}).")
                elif event_type == 'kline':
                    kline_start_time = event_data.get('k', {}).get('t') # K-line start timestamp
                    await self.kline_queues[symbol].put(RawMarketDataEvent(symbol, "kline", event_data, source_tag, kline_start_time))
                    self._update_last_received_data(symbol, "kline", event_data, source_tag, kline_start_time, current_timestamp_ns)
                    LOGGER.debug(f"MDG: {source_tag} -> K-line for {symbol} pushed (Start Time: {kline_start_time}).")
                else:
                    LOGGER.debug(f"MDG: Unhandled event type '{event_type}' from {source_tag} for {symbol}.")
            else:
                LOGGER.warning(f"MDG: Received non-standard or malformed WS message from {source_tag}: {message[:200]}...")

        except (json.JSONDecodeError, ValueError) as e:
            LOGGER.warning(f"MDG: Failed to decode/parse WS message from {source_tag}: {e}. Message snippet: {message[:100]}...", exc_info=True)
            await SHARED_STATE["alert_queue"].put(HealthCheckEvent("MarketDataGateway", "warning", f"JSON/Parse error on WS message: {e}", details={"source": source_tag, "message": message[:100]}))
        except Exception as e:
            LOGGER.error(f"MDG: Critical error in _process_raw_ws_message from {source_tag}: {e}", exc_info=True)
            await SHARED_STATE["alert_queue"].put(SystemAlertEvent("CRITICAL", f"MDG: Fatal error in raw message processing: {e}", details={"source": source_tag}))

    def _update_last_received_data(self, symbol: str, data_type: str, payload: Dict, source: str, sequence_id: Any, timestamp_ns: int):
        """
        Stores the latest data from each source for cross-validation.
        [Refonte: Stockage pour validation croisée]
        """
        key = f"{symbol}_{data_type}" # Unique key for each data stream type per symbol
        data_to_store = {"payload": payload, "timestamp_ns": timestamp_ns, "sequence_id": sequence_id}
        if source == "primary":
            self.last_primary_data[key] = data_to_store
        else: # secondary
            self.last_secondary_data[key] = data_to_store

    async def _monitor_data_integrity_loop(self):
        """
        Continuously monitors for data divergence between primary and secondary sources.
        Emits CRITICAL_DATA_MISMATCH alerts if divergence persists for a configured timeout.
        This is crucial for bot safety.
        [Refonte: Moteur de Séquençage et de Validation Croisée]
        """
        integrity_check_interval_seconds = 0.1 # Check very frequently (e.g., every 100ms)
        
        while self._running:
            await asyncio.sleep(integrity_check_interval_seconds)

            current_time_ns = time.perf_counter_ns()
            # Iterate over all known data stream keys (e.g., "BTC/USDT_trade_tick", "BTC/USDT_depth_update")
            keys_to_check = set(self.last_primary_data.keys()).union(self.last_secondary_data.keys())

            for key in list(keys_to_check): # Iterate over a copy to allow modification
                primary_data = self.last_primary_data.get(key)
                secondary_data = self.last_secondary_data.get(key)
                symbol, data_type = key.split('_', 1) # Extract symbol and data_type

                # Check if one source is missing or severely lagging
                primary_latency = (current_time_ns - self.last_ws_message_time.get(f"primary_{self.ws_uri_primary.split('/')[2].replace('.', '_')}", current_time_ns)) / 1_000_000
                secondary_latency = (current_time_ns - self.last_ws_message_time.get(f"secondary_{self.ws_uri_secondary.split('/')[2].replace('.', '_')}", current_time_ns)) / 1_000_000

                if primary_latency > self.data_divergence_timeout_ms * 2: # Primary source significantly delayed
                    if self.divergence_start_time_ns[key] == 0: # Only alert once
                         LOGGER.warning(f"MDG Integrity: Primary source for {key} has not sent data for {primary_latency:.0f}ms. Possible data stall.", extra={"symbol": symbol, "data_type": data_type, "source": "primary"})
                    self.divergence_start_time_ns[key] = current_time_ns # Start/reset divergence timer for this stall
                    # Don't proceed with price divergence check if one stream is stalled
                    continue
                elif secondary_latency > self.data_divergence_timeout_ms * 2: # Secondary source significantly delayed
                    if self.divergence_start_time_ns[key] == 0:
                        LOGGER.warning(f"MDG Integrity: Secondary source for {key} has not sent data for {secondary_latency:.0f}ms. Possible data stall.", extra={"symbol": symbol, "data_type": data_type, "source": "secondary"})
                    self.divergence_start_time_ns[key] = current_time_ns
                    continue
                
                # If both streams are "fresh" enough, proceed with content validation
                if not primary_data or not secondary_data:
                    # One source is missing its *last specific data point*, but connection might be live
                    # This could happen if, e.g., secondary source doesn't have trades for a symbol for a while
                    # Consider this a "minor" divergence, but don't critical alert yet.
                    # We might want to remove 'key' if no updates for too long.
                    continue

                divergence_detected = False
                message = ""
                details: Dict[str, Any] = {"symbol": symbol, "data_type": data_type, "source_primary_ts": primary_data['timestamp_ns'], "source_secondary_ts": secondary_data['timestamp_ns']}
                
                # Specific validation logic per data type
                if data_type == "trade_tick":
                    p_payload = primary_data['payload']
                    s_payload = secondary_data['payload']

                    p_price = to_float(p_payload.get('p'))
                    s_price = to_float(s_payload.get('p'))
                    p_qty = to_float(p_payload.get('q'))
                    s_qty = to_float(s_payload.get('q'))

                    # If trade IDs are identical, but prices/quantities differ -> critical
                    # This implies data corruption for a known trade
                    if primary_data['sequence_id'] == secondary_data['sequence_id'] and \
                       (abs(p_price - s_price) / max(p_price, s_price) > self.data_divergence_threshold_pct or \
                        abs(p_qty - s_qty) / max(p_qty, s_qty) > self.data_divergence_threshold_pct * 5): # Qty can vary more
                        
                        divergence_detected = True
                        message = f"Identical trade ID ({primary_data['sequence_id']}) but divergent prices/quantities for {symbol}."
                        details.update({"primary_price": p_price, "secondary_price": s_price, "price_div_pct": (abs(p_price-s_price)/max(p_price,s_price))*100 if max(p_price,s_price)>0 else 0,
                                        "primary_qty": p_qty, "secondary_qty": s_qty, "qty_div_pct": (abs(p_qty-s_qty)/max(p_qty,s_qty))*100 if max(p_qty,s_qty)>0 else 0})
                    # If trade IDs are different but prices are highly divergent (e.g., > 0.5% for recent trades)
                    elif primary_data['sequence_id'] != secondary_data['sequence_id'] and \
                         abs(p_price - s_price) / max(p_price, s_price) > self.data_divergence_threshold_pct * 5: # Higher threshold for different trades
                        divergence_detected = True
                        message = f"High price divergence between latest trades from different sources for {symbol}."
                        details.update({"primary_price": p_price, "secondary_price": s_price, "price_div_pct": (abs(p_price-s_price)/max(p_price,s_price))*100 if max(p_price,s_price)>0 else 0,
                                        "primary_trade_id": primary_data['sequence_id'], "secondary_trade_id": secondary_data['sequence_id']})

                elif data_type == "depth_update":
                    # For order book, focus on best bid/ask and possibly top 5 levels aggregated volume
                    p_bids_raw = primary_data['payload'].get('b', [])
                    p_asks_raw = primary_data['payload'].get('a', [])
                    s_bids_raw = secondary_data['payload'].get('b', [])
                    s_asks_raw = secondary_data['payload'].get('a', [])
                    
                    p_bids = {to_float(k): to_float(v) for k, v in p_bids_raw}
                    p_asks = {to_float(k): to_float(v) for k, v in p_asks_raw}
                    s_bids = {to_float(k): to_float(v) for k, v in s_bids_raw}
                    s_asks = {to_float(k): to_float(v) for k, v in s_asks_raw}
                    
                    p_best_bid = max(p_bids.keys()) if p_bids else 0
                    p_best_ask = min(p_asks.keys()) if p_asks else float('inf')
                    s_best_bid = max(s_bids.keys()) if s_bids else 0
                    s_best_ask = min(s_asks.keys()) if s_asks else float('inf')

                    if p_best_bid > 0 and s_best_bid > 0 and p_best_ask != float('inf') and s_best_ask != float('inf'):
                        bid_price_div = abs(p_best_bid - s_best_bid) / max(p_best_bid, s_best_bid) if max(p_best_bid,s_best_bid) > 0 else 0
                        ask_price_div = abs(p_best_ask - s_best_ask) / max(p_best_ask,s_best_ask) if max(p_best_ask,s_best_ask) > 0 else 0

                        # Check divergence for best bid/ask prices
                        if bid_price_div > self.data_divergence_threshold_pct or \
                           ask_price_div > self.data_divergence_threshold_pct:
                            divergence_detected = True
                            message = f"Best Bid/Ask price divergence for {symbol}."
                            details.update({"primary_bb": p_best_bid, "secondary_bb": s_best_bid, "bb_div_pct": bid_price_div*100,
                                            "primary_ba": p_best_ask, "secondary_ba": s_best_ask, "ba_div_pct": ask_price_div*100})
                            
                        # Also, check divergence for top N aggregated volumes (conceptual)
                        # sum_p_top_qty = sum(q for p, q in list(p_bids.items())[:5] + list(p_asks.items())[:5])
                        # sum_s_top_qty = sum(q for p, q in list(s_bids.items())[:5] + list(s_asks.items())[:5])
                        # if sum_p_top_qty > 0 and sum_s_top_qty > 0 and abs(sum_p_top_qty - sum_s_top_qty) / max(sum_p_top_qty, sum_s_top_qty) > self.data_divergence_threshold_pct * 5:
                        #     divergence_detected = True
                        #     message += f" Top 5 level volume divergence for {symbol}."


                # Apply persistence check for divergence
                if divergence_detected:
                    if self.divergence_start_time_ns[key] == 0: # First detection of divergence
                        self.divergence_start_time_ns[key] = current_time_ns
                        LOGGER.warning(f"MDG: Initial data divergence detected for {key}. Monitoring persistence. Details: {message}", extra=details)
                    elif (current_time_ns - self.divergence_start_time_ns[key]) / 1_000_000 > self.data_divergence_timeout_ms:
                        # CRITICAL alert: divergence persisted beyond timeout
                        LOGGER.critical(f"MDG: CRITICAL DATA MISMATCH for {key}! Divergence persisted for {(current_time_ns - self.divergence_start_time_ns[key]) / 1_000_000:.0f}ms. {message}")
                        async with STATE_LOCK:
                            SHARED_STATE["health_checks"]["data_divergence_alerts"][key] += 1
                        await SHARED_STATE["alert_queue"].put(SystemAlertEvent("CRITICAL", self.config["monitoring"]["alert_templates"]["data_mismatch"].format(
                            message=message, primary_val=details.get('primary_price', details.get('primary_bb')), 
                            secondary_val=details.get('secondary_price', details.get('secondary_bb')), 
                            divergence_pct=details.get('price_div_pct', details.get('bb_div_pct'))
                        ), details=details))
                        # Trigger system kill switch or pause specific module/strategy
                        await SHARED_STATE["event_queue"].put(BotCommandEvent("ACTIVATE_GLOBAL_KILL_SWITCH", details={"reason": f"CRITICAL_DATA_MISMATCH_{key}"}))
                        self.divergence_start_time_ns[key] = 0 # Reset to prevent immediate re-trigger
                else:
                    self.divergence_start_time_ns[key] = 0 # Reset timer if divergence is resolved

    async def stop(self):
        """Gracefully stops the MarketDataGateway and its associated tasks."""
        self._running = False
        LOGGER.info("MarketDataGateway stop requested. Cancelling WebSocket connections and integrity monitor.")
        for task in self.ws_connections.values():
            task.cancel()
        if self._data_integrity_monitor_task:
            self._data_integrity_monitor_task.cancel()
        
        # Drain queues by putting None sentinel for downstream modules to stop processing
        for symbol in self.active_symbols:
            await self.tick_queues[symbol].put(None) 
            await self.orderbook_queues[symbol].put(None)
            await self.kline_queues[symbol].put(None)

        try:
            await asyncio.gather(
                *list(self.ws_connections.values()), 
                self._data_integrity_monitor_task,
                return_exceptions=True
            )
        except asyncio.CancelledError:
            LOGGER.info("MarketDataGateway: All tasks cancelled successfully during stop.")
        except Exception as e:
            LOGGER.error(f"MarketDataGateway: Error during graceful stop: {e}", exc_info=True)


# --- NEW: FeatureEngine (the cerebral cortex) ---
class FeatureEngine:
    """
    FeatureEngine: The cerebral cortex of the bot. It transforms raw market data
    into a coherent, multi-dimensional vision by calculating sophisticated features
    across various time granularities and types.
    [Refonte: Création du module, Architecture en cascade]
    """
    def __init__(self, config: Dict):
        self.config = config
        self.exchange_config = config["exchange"]
        self.research_config = config["research"]
        self.active_symbols = self.exchange_config["active_symbols"]
        self.max_ohlcv_history = self.exchange_config["max_ohlcv_history_for_features"]
        
        # Subscribe to raw data queues from MarketDataGateway
        self.tick_queues = SHARED_STATE["tick_data_queue"]
        self.orderbook_queues = SHARED_STATE["orderbook_data_queue"]
        
        # Internal buffers for data reconstruction and feature calculation
        self.ohlcv_reconstruction_buffers = defaultdict(lambda: defaultdict(list)) # {symbol: {timeframe: [raw_candles]}}
        self.current_partial_candles: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict)) # {symbol: {timeframe: {open, high, low, close, volume, start_ts}}}
        self.last_closed_ohlcv_candle: Dict[str, Dict[str, pd.Series]] = defaultdict(dict) # {symbol: {timeframe: last_closed_candle_series}}
        
        # [Refonte: Micro-bougies] Buffers for generating volume, tick, and dollar bars
        self.volume_bar_buffers = defaultdict(lambda: {'current_volume_usd': 0.0, 'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'start_time': None, 'num_trades': 0, 'total_buy_volume_usd': 0.0, 'total_sell_volume_usd': 0.0})
        self.tick_bar_buffers = defaultdict(lambda: {'current_ticks': 0, 'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'start_time': None, 'num_trades': 0})
        self.dollar_bar_buffers = defaultdict(lambda: {'current_dollar_volume_usd': 0.0, 'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'start_time': None, 'num_trades': 0, 'total_buy_volume_usd': 0.0, 'total_sell_volume_usd': 0.0})

        # Feature scaling and ML models for GARCH (for volatility prediction as a feature)
        self.feature_scaler: Optional[Union[StandardScaler, MinMaxScaler]] = None
        self.garch_models = defaultdict(dict) 
        
        self.load_ml_assets_for_features() # Loads pre-trained scaler and GARCH models
        
        self._running = False
        self.feature_calculation_tasks: Dict[str, asyncio.Task] = {}
        self.raw_data_processing_tasks: List[asyncio.Task] = []
        self.last_feature_calc_time = defaultdict(float) # Track last calculation time per symbol

        # For adaptive normalization (e.g., Z-score on a rolling window)
        self.rolling_feature_stats = defaultdict(lambda: defaultdict(deque)) # {symbol: {feature_name: deque of recent values}}

        LOGGER.info("FeatureEngine initialized. Ready for feature computation.")

    def load_ml_assets_for_features(self):
        """Loads pre-trained feature scaler and GARCH models from disk."""
        scaler_path = CONFIG["research"]["feature_scaler_save_path"]
        if os.path.exists(scaler_path):
            try:
                self.feature_scaler = joblib.load(scaler_path)
                LOGGER.info(f"FeatureEngine: Feature scaler loaded from {scaler_path}.")
            except Exception as e:
                LOGGER.error(f"FeatureEngine: Error loading feature scaler: {e}. Features will not be scaled.", exc_info=True)
        
        garch_model_path = CONFIG["research"]["garch_model_save_path"]
        if os.path.exists(garch_model_path):
            try:
                loaded_garch_models = joblib.load(garch_model_path)
                for symbol, tf_models in loaded_garch_models.items():
                    for tf, model in tf_models.items():
                        self.garch_models[symbol][tf] = model
                        LOGGER.info(f"FeatureEngine: GARCH model for {symbol} {tf} loaded.")
            except Exception as e:
                LOGGER.error(f"FeatureEngine: Error loading GARCH models: {e}. GARCH volatility feature may not be available.", exc_info=True)

    async def run(self):
        """Starts the FeatureEngine, launching tasks to process raw data and calculate features."""
        self._running = True
        LOGGER.info("FeatureEngine starting. Launching raw data processing and feature calculation loops.")
        
        # Launch tasks to consume from MarketDataGateway's queues for each active symbol
        for sym in self.active_symbols:
            self.raw_data_processing_tasks.append(asyncio.create_task(
                self._process_raw_tick_data_from_queue(sym), name=f"FE_TickProcessor-{sym}"
            ))
            self.raw_data_processing_tasks.append(asyncio.create_task(
                self._process_raw_orderbook_data_from_queue(sym), name=f"FE_OBProcessor-{sym}"
            ))
            # If you subscribe to raw kline streams from MDG, add a processor here:
            # self.raw_data_processing_tasks.append(asyncio.create_task(
            #     self._process_raw_kline_data_from_queue(sym), name=f"FE_KlineProcessor-{sym}"
            # ))
            
            # Launch a dedicated feature calculation loop for each symbol, running periodically
            self.feature_calculation_tasks[sym] = asyncio.create_task(
                self._feature_calculation_loop(sym), name=f"FE_FeatureCalc-{sym}"
            )

        # Wait for all FeatureEngine's tasks to complete (or be cancelled)
        await asyncio.gather(
            *self.raw_data_processing_tasks,
            *self.feature_calculation_tasks.values(),
            return_exceptions=True
        )
        LOGGER.info("FeatureEngine stopped.")

    async def _process_raw_tick_data_from_queue(self, symbol: str):
        """
        Consumes raw trade ticks from the MarketDataGateway queue for a given symbol.
        This is the entry point for tick-level data processing.
        [Refonte: Couche 1 - Tick-level processing]
        """
        while self._running:
            try:
                event: RawMarketDataEvent = await self.tick_queues[symbol].get()
                if event is None: break # Sentinel for shutdown
                
                payload = event.payload
                price = to_float(payload['p'])
                volume = to_float(payload['q'])
                trade_time_ms = to_int(payload['T'])
                is_buyer_maker = payload['m'] # True if taker buy, false if taker sell

                if price <= 0 or volume <= 0:
                    LOGGER.warning(f"FE: Invalid trade tick received (price={price}, volume={volume}) for {symbol}. Skipping.", extra={"symbol": symbol, "payload": payload})
                    self.tick_queues[symbol].task_done()
                    continue
                
                # Store raw tick in SHARED_STATE for broader access (e.g. for VPIN calculation or debug)
                async with STATE_LOCK:
                    SHARED_STATE["tick_data"][symbol].append({
                        'price': price, 'volume': volume, 'timestamp': trade_time_ms, 'is_buyer_maker': is_buyer_maker
                    })
                    SHARED_STATE["current_utc_ms"] = max(SHARED_STATE["current_utc_ms"], trade_time_ms) # Update global high-res time
                    SHARED_STATE["telemetry_data"]["ticks_processed_fe"] = SHARED_STATE["telemetry_data"].get("ticks_processed_fe", 0) + 1

                # Reconstruct standard OHLCV candles and micro-bars from this single tick
                await self._reconstruct_all_ohlcv_and_micro_bars(symbol, price, volume, is_buyer_maker, trade_time_ms)
                
                self.tick_queues[symbol].task_done()

            except asyncio.CancelledError:
                LOGGER.debug(f"FE: Raw tick data processing for {symbol} cancelled.")
                break
            except Exception as e:
                LOGGER.error(f"FE: Critical error processing raw tick data for {symbol}: {e}", exc_info=True)
                await SHARED_STATE["alert_queue"].put(HealthCheckEvent("FeatureEngine", "critical", f"Tick processing error: {e}", details={"symbol": symbol, "queue_size": self.tick_queues[symbol].qsize()}))
                self.tick_queues[symbol].task_done() # Mark done even on error to prevent queue clog

    async def _process_raw_orderbook_data_from_queue(self, symbol: str):
        """
        Consumes raw order book updates from the MarketDataGateway queue for a given symbol.
        Applies updates to the in-memory order book snapshot.
        [Refonte: Couche 1 - Orderbook processing]
        """
        while self._running:
            try:
                event: RawMarketDataEvent = await self.orderbook_queues[symbol].get()
                if event is None: break # Sentinel for shutdown
                
                data = event.payload
                async with STATE_LOCK:
                    current_ob = SHARED_STATE["order_book_snapshots"][symbol]
                    new_timestamp = to_int(data.get('E', get_current_utc_ms())) # Event time if available, else current time
                    last_update_id = to_int(data.get('u', data.get('lastUpdateId', 0))) # Binance futures uses 'u', spot uses 'lastUpdateId'
                    
                    # Basic sequence check for order book integrity (critical for delta updates)
                    if current_ob["last_update_id"] != 0 and last_update_id < current_ob["last_update_id"]:
                         LOGGER.warning(f"FE: Out-of-sequence OB update for {symbol}. Current last_update_id: {current_ob['last_update_id']}, Received: {last_update_id}. Skipping to maintain integrity.", extra={"symbol": symbol})
                         self.orderbook_queues[symbol].task_done()
                         continue
                    
                    # Apply bids updates (bids: [[price, quantity], ...])
                    for bid in data.get('b', []):
                        price = to_float(bid[0])
                        qty = to_float(bid[1])
                        if qty == 0: # Quantity of 0 means remove this price level
                            current_ob["bids"].pop(price, None)
                        else:
                            current_ob["bids"][price] = qty
                    
                    # Apply asks updates (asks: [[price, quantity], ...])
                    for ask in data.get('a', []):
                        price = to_float(ask[0])
                        qty = to_float(ask[1])
                        if qty == 0: # Quantity of 0 means remove this price level
                            current_ob["asks"].pop(price, None)
                        else:
                            current_ob["asks"][price] = qty
                    
                    current_ob["last_update_id"] = last_update_id
                    current_ob["timestamp"] = new_timestamp

                    # Maintain a limited depth for performance and relevance
                    current_ob["bids"] = dict(sorted(current_ob["bids"].items(), reverse=True)[:self.exchange_config["max_orderbook_depth_storage"]])
                    current_ob["asks"] = dict(sorted(current_ob["asks"].items())[:self.exchange_config["max_orderbook_depth_storage"]])

                    SHARED_STATE["telemetry_data"]["orderbook_updates_processed_fe"] = SHARED_STATE["telemetry_data"].get("orderbook_updates_processed_fe", 0) + 1
                    LOGGER.debug(f"FE: Orderbook for {symbol} updated (Last ID: {last_update_id}). Bids: {len(current_ob['bids'])}, Asks: {len(current_ob['asks'])}")
                
                self.orderbook_queues[symbol].task_done()

            except asyncio.CancelledError:
                LOGGER.debug(f"FE: Raw orderbook data processing for {symbol} cancelled.")
                break
            except Exception as e:
                LOGGER.error(f"FE: Critical error processing raw orderbook data for {symbol}: {e}", exc_info=True)
                await SHARED_STATE["alert_queue"].put(HealthCheckEvent("FeatureEngine", "critical", f"Orderbook processing error: {e}", details={"symbol": symbol, "queue_size": self.orderbook_queues[symbol].qsize()}))
                self.orderbook_queues[symbol].task_done()

    # async def _process_raw_kline_data_from_queue(self, symbol: str):
    #     """
    #     (Conceptual) Consumes raw kline data directly from the exchange's kline stream.
    #     If MDG provides klines, FeatureEngine can consume them rather than reconstruct.
    #     """
    #     while self._running:
    #         try:
    #             event: RawMarketDataEvent = await self.kline_queues[symbol].get()
    #             if event is None: break # Sentinel
    #             # Directly update SHARED_STATE["ohlcv_data"][symbol][timeframe] here
    #             # Add logic for validating and appending the kline, maintaining max_ohlcv_history
    #             self.kline_queues[symbol].task_done()
    #         except asyncio.CancelledError: break
    #         except Exception as e: LOGGER.error(f"FE: Error processing raw kline for {symbol}: {e}"); self.kline_queues[symbol].task_done()


    async def _reconstruct_all_ohlcv_and_micro_bars(self, symbol: str, price: float, volume: float, is_buyer_maker: bool, trade_time_ms: int):
        """
        Reconstructs standard OHLCV candles across multiple timeframes (Couche 2: Temps-agrégé)
        and generates micro-bars (volume, tick, dollar bars) (Couche 2: Micro-bougies).
        [Refonte: Reconstruction de bougies multi-types]
        """
        # --- Standard OHLCV Reconstruction (Time-based Bars) ---
        for tf_str, tf_sec in {tf: timeframe_to_seconds(tf) for tf in self.config["exchange"]["default_timeframes_for_ohlcv_reconstruction"]}.items():
            if tf_sec == 0: continue # Skip if timeframe conversion failed
            
            candle_start_time_ms = (trade_time_ms // (tf_sec * 1000)) * (tf_sec * 1000) # Align to candle start
            
            async with STATE_LOCK:
                # Retrieve current OHLCV DataFrame (optimized to get a view/reference for performance)
                ohlcv_df_ref = SHARED_STATE["ohlcv_data"][symbol][tf_str] # Direct reference
                
                is_new_candle = ohlcv_df_ref.empty or candle_start_time_ms > ohlcv_df_ref.index[-1].timestamp() * 1000
                
                if is_new_candle:
                    # If previous candle exists and is complete, close it and emit an update event
                    if not ohlcv_df_ref.empty and ohlcv_df_ref.index[-1].timestamp() * 1000 < candle_start_time_ms:
                        last_closed_candle_series = ohlcv_df_ref.iloc[-1].copy() # Copy to avoid altering original DF
                        last_closed_candle_series.name = ohlcv_df_ref.index[-1] # Keep the original timestamp as index name
                        self.last_closed_ohlcv_candle[symbol][tf_str] = last_closed_candle_series # Store for feature lookups
                        
                        # Emit MarketDataUpdateEvent for the *closed* OHLCV candle for downstream modules
                        # CORRECTION: Ces deux lignes doivent être indentées à l'intérieur du "if not ohlcv_df_ref.empty..."
                        await SHARED_STATE["event_queue"].put(MarketDataEvent(symbol, "ohlcv", last_closed_candle_series))
                        LOGGER.debug(f"FE: Closed {tf_str} candle for {symbol} at {last_closed_candle_series.name} (Close: {last_closed_candle_series['close']:.4f}, Volume: {last_closed_candle_series['volume']:.2f})")

                    # Initialize a new candle with the current tick's data
                    new_candle_series = pd.Series({
                        'open': price, 'high': price, 'low': price, 'close': price, 'volume': volume
                    }, name=pd.to_datetime(candle_start_time_ms, unit='ms', utc=True))
                    
                    # Append new candle, maintaining max history efficiently
                    new_df_row = pd.DataFrame([new_candle_series])
                    # Using pd.concat for append; tail(N) ensures fixed size and avoids excessive memory
                    SHARED_STATE["ohlcv_data"][symbol][tf_str] = pd.concat([ohlcv_df_ref, new_df_row]).tail(self.max_ohlcv_history)
                    
                else:
                    # Update the current (last) candle with new tick data
                    current_candle_name = ohlcv_df_ref.index[-1]
                    
                    # Update OHLCV fields directly on the DataFrame row using .loc for safety
                    SHARED_STATE["ohlcv_data"][symbol][tf_str].loc[current_candle_name, 'high'] = max(ohlcv_df_ref.loc[current_candle_name, 'high'], price)
                    SHARED_STATE["ohlcv_data"][symbol][tf_str].loc[current_candle_name, 'low'] = min(ohlcv_df_ref.loc[current_candle_name, 'low'], price)
                    SHARED_STATE["ohlcv_data"][symbol][tf_str].loc[current_candle_name, 'close'] = price
                    SHARED_STATE["ohlcv_data"][symbol][tf_str].loc[current_candle_name, 'volume'] += volume
                    
        # --- Micro-Bar Reconstruction (Volume, Tick, Dollar Bars) ---
        # These are event-driven bars, closing when a certain condition is met, not on time.
        # [Refonte: Couche 2 - Micro-bougies]

        # 1. Volume Bar: Aggregates trades until a certain USD volume threshold is met
        volume_bar_buffer = self.volume_bar_buffers[symbol]
        current_tick_dollar_volume = volume * price

        if volume_bar_buffer['open'] is None: # Initialize the first tick of a new bar
            volume_bar_buffer.update({'open': price, 'high': price, 'low': price, 'start_time': trade_time_ms})
        
        volume_bar_buffer['high'] = max(volume_bar_buffer['high'], price)
        volume_bar_buffer['low'] = min(volume_bar_buffer['low'], price)
        volume_bar_buffer['close'] = price # Last trade price
        volume_bar_buffer['current_volume_usd'] += current_tick_dollar_volume
        volume_bar_buffer['num_trades'] += 1
        
        if is_buyer_maker:
            volume_bar_buffer['total_buy_volume_usd'] += current_tick_dollar_volume
        else:
            volume_bar_buffer['total_sell_volume_usd'] += current_tick_dollar_volume

        if volume_bar_buffer['current_volume_usd'] >= self.research_config["volume_bar_size_usd"]:
            bar_data = {
                'open': volume_bar_buffer['open'], 'high': volume_bar_buffer['high'], 'low': volume_bar_buffer['low'], 'close': volume_bar_buffer['close'],
                'volume_usd': volume_bar_buffer['current_volume_usd'], 'start_time': volume_bar_buffer['start_time'], 'end_time': trade_time_ms,
                'num_trades': volume_bar_buffer['num_trades'],
                'buy_volume_usd': volume_bar_buffer['total_buy_volume_usd'],
                'sell_volume_usd': volume_bar_buffer['total_sell_volume_usd']
            }
            # CORRECTION: Ce bloc entier (async with, await, logger, reset) doit être indenté à l'intérieur du "if"
            async with STATE_LOCK:
                SHARED_STATE["micro_bar_data"][symbol]["volume_bar"].append(bar_data)
                # Assurer une taille fixe pour une gestion efficace de la mémoire
                while len(SHARED_STATE["micro_bar_data"][symbol]["volume_bar"]) > 1000: # Historique max de 1000 barres de volume
                    SHARED_STATE["micro_bar_data"][symbol]["volume_bar"].popleft()

            await SHARED_STATE["event_queue"].put(MarketDataEvent(symbol, "micro_bar", {"type": "volume_bar", "payload": bar_data}))
            LOGGER.debug(f"FE: Generated Volume Bar for {symbol}. Total Volume: {bar_data['volume_usd']:.2f} USD.")

            # Réinitialiser le tampon pour la prochaine barre de volume
            self.volume_bar_buffers[symbol] = {
                'current_volume_usd': 0.0, 'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'start_time': None,
                'num_trades': 0, 'total_buy_volume_usd': 0.0, 'total_sell_volume_usd': 0.0
            }

        # 2. Tick Bar: Agrège un nombre fixe de transactions (ticks)
        tick_bar_buffer = self.tick_bar_buffers[symbol]
        if tick_bar_buffer['open'] is None:
            tick_bar_buffer.update({'open': price, 'high': price, 'low': price, 'start_time': trade_time_ms})

        tick_bar_buffer['high'] = max(tick_bar_buffer['high'], price)
        tick_bar_buffer['low'] = min(tick_bar_buffer['low'], price)
        tick_bar_buffer['close'] = price
        tick_bar_buffer['current_ticks'] += 1
        tick_bar_buffer['num_trades'] += 1

        if tick_bar_buffer['current_ticks'] >= self.research_config["tick_bar_size"]:
            bar_data = {
                'open': tick_bar_buffer['open'], 'high': tick_bar_buffer['high'], 'low': tick_bar_buffer['low'], 'close': tick_bar_buffer['close'],
                'num_ticks': tick_bar_buffer['current_ticks'], 'start_time': tick_bar_buffer['start_time'], 'end_time': trade_time_ms,
                'num_trades': tick_bar_buffer['num_trades']
            }
            # CORRECTION: Ce bloc entier est maintenant correctement indenté à l'intérieur du "if"
            async with STATE_LOCK:
                SHARED_STATE["micro_bar_data"][symbol]["tick_bar"].append(bar_data)
                while len(SHARED_STATE["micro_bar_data"][symbol]["tick_bar"]) > 1000: # Max 1000 tick bars history
                    SHARED_STATE["micro_bar_data"][symbol]["tick_bar"].popleft()

                await SHARED_STATE["event_queue"].put(MarketDataEvent(symbol, "micro_bar", {"type": "tick_bar", "payload": bar_data}))
                LOGGER.debug(f"FE: Generated Tick Bar for {symbol}. Ticks: {bar_data['num_ticks']}.")
            # Reset buffer
            self.tick_bar_buffers[symbol] = {'current_ticks': 0, 'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'start_time': None, 'num_trades': 0}
        
        # 3. Dollar Bar: Aggregates trades until a certain total dollar volume (sum of price * quantity) is met
        dollar_bar_buffer = self.dollar_bar_buffers[symbol]
        if dollar_bar_buffer['open'] is None:
            dollar_bar_buffer.update({'open': price, 'high': price, 'low': price, 'start_time': trade_time_ms})
        
        dollar_bar_buffer['high'] = max(dollar_bar_buffer['high'], price)
        dollar_bar_buffer['low'] = min(dollar_bar_buffer['low'], price)
        dollar_bar_buffer['close'] = price
        dollar_bar_buffer['current_dollar_volume_usd'] += current_tick_dollar_volume
        dollar_bar_buffer['num_trades'] += 1

        if dollar_bar_buffer['current_dollar_volume_usd'] >= self.research_config["dollar_bar_size_usd"]:
            bar_data = {
                'open': dollar_bar_buffer['open'], 'high': dollar_bar_buffer['high'], 'low': dollar_bar_buffer['low'], 'close': dollar_bar_buffer['close'],
                'dollar_volume_usd': dollar_bar_buffer['current_dollar_volume_usd'], 'start_time': dollar_bar_buffer['start_time'], 'end_time': trade_time_ms,
                'num_trades': dollar_bar_buffer['num_trades']
            }
            # CORRECTION: Ce bloc entier est maintenant correctement indenté à l'intérieur du "if"
            async with STATE_LOCK:
                SHARED_STATE["micro_bar_data"][symbol]["dollar_bar"].append(bar_data)
                while len(SHARED_STATE["micro_bar_data"][symbol]["dollar_bar"]) > 1000: # Max 1000 dollar bars history
                    SHARED_STATE["micro_bar_data"][symbol]["dollar_bar"].popleft()

                await SHARED_STATE["event_queue"].put(MarketDataEvent(symbol, "micro_bar", {"type": "dollar_bar", "payload": bar_data}))
                LOGGER.debug(f"FE: Generated Dollar Bar for {symbol}. Total Dollar Volume: {bar_data['dollar_volume_usd']:.2f} USD.")
            # Reset buffer
            self.dollar_bar_buffers[symbol] = {'current_dollar_volume_usd': 0.0, 'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'start_time': None, 'num_trades': 0}
    async def _feature_calculation_loop(self, symbol: str):
        """
        Periodically calculates advanced features for a given symbol and emits a FeatureEvent.
        This loop is responsible for the "Couche 3: Temps-agrégé" and other higher-level features.
        [Refonte: Calcul des features en cascade]
        """
        feature_interval_sec = 1 # How frequently features are recalculated (e.g., every second)
        while self._running:
            await asyncio.sleep(feature_interval_sec)
            
            current_time_ms = get_current_utc_ms()
            # Prevent re-calculation if the interval hasn't passed since last successful calculation
            if current_time_ms / 1000 - self.last_feature_calc_time[symbol] < feature_interval_sec * 0.9:
                continue

            # This is where the core feature computation happens.
            # It will access SHARED_STATE (ohlcv_data, tick_data, micro_bar_data, order_book_snapshots)
            # which are populated by the raw data processors.
            features = await self._calculate_all_advanced_features(symbol, current_time_ms)
            
            if features:
                # Store the latest features in SHARED_STATE for AlphaModel to consume
                async with STATE_LOCK:
                    SHARED_STATE["features_data"][symbol] = features
                # Emit a FeatureEvent for downstream modules (e.g., AlphaModel)
                await SHARED_STATE["event_queue"].put(FeatureEvent(symbol, features, current_time_ms))
                self.last_feature_calc_time[symbol] = current_time_ms / 1000
                async with STATE_LOCK:
                    SHARED_STATE["telemetry_data"]["features_calculated"] = SHARED_STATE["telemetry_data"].get("features_calculated", 0) + 1
            else:
                LOGGER.warning(f"FE: No features calculated for {symbol} in this cycle, possibly due to insufficient data.")


    async def _calculate_all_advanced_features(self, symbol: str, current_time_ms: int) -> Optional[Dict]:
        """
        Calculates all advanced quantitative features for a given symbol.
        This method leverages data from various granularities (ticks, micro-bars, OHLCV).
        [Refonte: Bibliothèque de Features Quantitatives Exotiques]
        """
        features_dict = {}
        
        # Retrieve snapshots of data from SHARED_STATE (read-only access under lock)
        async with STATE_LOCK:
            # Note: `tick_data` can be very large, access it efficiently or slice for recent data
            tick_data_deque = SHARED_STATE["tick_data"][symbol] 
            order_book = copy.deepcopy(SHARED_STATE["order_book_snapshots"][symbol])
            ohlcv_data_snapshot = {
                tf_str: df.copy() for tf_str, df in SHARED_STATE["ohlcv_data"][symbol].items()
            }
            micro_bar_data_snapshot = {
                bar_type: list(bar_deque) for bar_type, bar_deque in SHARED_STATE["micro_bar_data"][symbol].items()
            }
        
        # Ensure sufficient fundamental data (e.g., 1-minute candles) is available
        if "1m" not in ohlcv_data_snapshot or ohlcv_data_snapshot["1m"].empty or len(ohlcv_data_snapshot["1m"]) < 30:
            LOGGER.debug(f"FE: Not enough 1m OHLCV data for {symbol} to calculate most features. Skipping.")
            return None

        # Prepare base DataFrame for indicators (usually 1-minute or 5-minute close prices)
        df_1m_base = ohlcv_data_snapshot["1m"]
        close_1m = df_1m_base['close'].values
        high_1m = df_1m_base['high'].values
        low_1m = df_1m_base['low'].values
        open_1m = df_1m_base['open'].values

        # --- Order Flow Imbalance (OFI) ---
        # OFI on last X volume bars: Measure directional volume across event-driven bars
        if "volume_bar" in micro_bar_data_snapshot and len(micro_bar_data_snapshot["volume_bar"]) >= 10:
            recent_volume_bars = micro_bar_data_snapshot["volume_bar"][-10:]
            total_buy_vol_usd = sum(b['buy_volume_usd'] for b in recent_volume_bars)
            total_sell_vol_usd = sum(b['sell_volume_usd'] for b in recent_volume_bars)
            total_bar_vol_usd = total_buy_vol_usd + total_sell_vol_usd
            features_dict["OFI_10_vol_bar"] = (total_buy_vol_usd - total_sell_vol_usd) / total_bar_vol_usd if total_bar_vol_usd > 0 else 0
        else: features_dict["OFI_10_vol_bar"] = 0.0

        # OFI on 1m candles: Simple aggregation of volume for up/down candles
        if len(df_1m_base) >= 10:
            df_1m_ofi = df_1m_base.iloc[-10:].copy()
            df_1m_ofi['change'] = df_1m_ofi['close'].diff()
            # Simple assumption: candle up = buy volume, candle down = sell volume
            df_1m_ofi['buy_vol'] = df_1m_ofi.apply(lambda row: row['volume'] if row['change'] > 0 else 0, axis=1)
            df_1m_ofi['sell_vol'] = df_1m_ofi.apply(lambda row: row['volume'] if row['change'] < 0 else 0, axis=1)
            total_vol_1m = df_1m_ofi['volume'].sum()
            features_dict["OFI_1m_candle"] = (df_1m_ofi['buy_vol'].sum() - df_1m_ofi['sell_vol'].sum()) / total_vol_1m if total_vol_1m > 0 else 0.0
        else: features_dict["OFI_1m_candle"] = 0.0

        # --- VWAP Deviation (Volume-Weighted Average Price) ---
        if "5m" in ohlcv_data_snapshot and not ohlcv_data_snapshot["5m"].empty and len(ohlcv_data_snapshot["5m"]) >= 20:
            df_5m_vwap = ohlcv_data_snapshot["5m"].iloc[-20:].copy()
            df_5m_vwap['tpv'] = df_5m_vwap['close'] * df_5m_vwap['volume']
            vwap = df_5m_vwap['tpv'].sum() / df_5m_vwap['volume'].sum() if df_5m_vwap['volume'].sum() > 0 else df_5m_vwap['close'].iloc[-1]
            features_dict["VWAP_Dev_5m"] = (df_5m_vwap['close'].iloc[-1] - vwap) / vwap if vwap > 0 else 0.0
        else: features_dict["VWAP_Dev_5m"] = 0.0

        # --- Momentum (Slope of Price) ---
        for period_mins in [10, 60]:
            tf_str = f"{period_mins}m"
            if tf_str in ohlcv_data_snapshot and not ohlcv_data_snapshot[tf_str].empty and len(ohlcv_data_snapshot[tf_str]) >= 20:
                df_tf_momentum = ohlcv_data_snapshot[tf_str].iloc[-20:].copy() # Use last 20 candles for slope
                if len(df_tf_momentum) >= 2: # Need at least 2 points for a slope
                    slope = np.polyfit(range(len(df_tf_momentum)), df_tf_momentum['close'], 1)[0]
                    features_dict[f"Momentum_{period_mins}m_slope"] = slope
                else: features_dict[f"Momentum_{period_mins}m_slope"] = 0.0
            else: features_dict[f"Momentum_{period_mins}m_slope"] = 0.0

        # --- Volatility (GARCH Predicted) --- (Couche 3: Temps-agrégé)
        # Uses pre-trained GARCH models (loaded during init) to predict next period's volatility.
        if "1h" in ohlcv_data_snapshot and not ohlcv_data_snapshot["1h"].empty and len(ohlcv_data_snapshot["1h"]) > 100:
            df_1h_garch = ohlcv_data_snapshot["1h"]
            returns_1h = 100 * df_1h_garch['close'].pct_change().dropna()
            if len(returns_1h) > 50:
                try:
                    if symbol in self.garch_models and "1h" in self.garch_models[symbol]:
                        garch_fit_result = self.garch_models[symbol]["1h"]
                        # For live prediction, assume the model instance is updated or re-fitted incrementally
                        # Here, we simulate prediction based on the last fitted state.
                        forecast = garch_fit_result.forecast(horizon=1, start=len(returns_1h)-1)
                        predicted_vol = float(forecast.variance.iloc[-1].values[0])
                        features_dict["Volatility_GARCH_Predicted_1h"] = predicted_vol
                    else:
                        LOGGER.debug(f"FE: GARCH model for {symbol} 1h not available/loaded for live prediction.")
                        features_dict["Volatility_GARCH_Predicted_1h"] = 0.0
                except Exception as e:
                    LOGGER.warning(f"FE: Error predicting GARCH volatility for {symbol} 1h: {e}")
                    features_dict["Volatility_GARCH_Predicted_1h"] = 0.0
            else: features_dict["Volatility_GARCH_Predicted_1h"] = 0.0
        else: features_dict["Volatility_GARCH_Predicted_1h"] = 0.0

        # --- Standard Technical Indicators (RSI, MACD, Bollinger Bands) ---
        if len(df_1m_base) >= 30: # Minimum data required for these indicators
            rsi = talib.RSI(close_1m, timeperiod=14)
            features_dict["RSI_14_last"] = rsi[-1] if not np.isnan(rsi[-1]) else 0.0

            macd, macdsignal, macdhist = talib.MACD(close_1m, fastperiod=12, slowperiod=26, signalperiod=9)
            features_dict["MACD_Hist_last"] = macdhist[-1] if not np.isnan(macdhist[-1]) else 0.0

            upper_bb, mid_bb, lower_bb = talib.BBANDS(close_1m, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            bb_width = (upper_bb[-1] - lower_bb[-1]) / mid_bb[-1] * 100 if mid_bb[-1] > 0 and not np.isnan(mid_bb[-1]) else 0.0
            features_dict["BB_Width_1m_Pct"] = bb_width
        else:
            features_dict["RSI_14_last"] = 0.0
            features_dict["MACD_Hist_last"] = 0.0
            features_dict["BB_Width_1m_Pct"] = 0.0

        # --- Candlestick Patterns ---
        if len(df_1m_base) >= 5: # Need at least 5 candles for some patterns
            open_p, high_p, low_p, close_p = open_1m[-5:], high_1m[-5:], low_1m[-5:], close_1m[-5:] # Last 5 candles for patterns
            doji = talib.CDLDOJI(open_p, high_p, low_p, close_p)
            features_dict["Price_Action_Pattern_Doji"] = 1 if doji[-1] != 0 else 0.0
        else: features_dict["Price_Action_Pattern_Doji"] = 0.0

        # --- Orderbook Imbalance (Top N levels) ---
        if order_book.get("bids") and order_book.get("asks"):
            # Sum volumes for top 10 bid/ask levels
            bids_sum_top_N_qty = sum(qty for _, qty in list(order_book["bids"].items())[:10])
            asks_sum_top_N_qty = sum(qty for _, qty in list(order_book["asks"].items())[:10])
            total_sum_ob_vol = bids_sum_top_N_qty + asks_sum_top_N_qty
            
            features_dict["Orderbook_Imbalance_Top10_Ratio"] = (bids_sum_top_N_qty - asks_sum_top_N_qty) / total_sum_ob_vol if total_sum_ob_vol > 0 else 0.0
        else: features_dict["Orderbook_Imbalance_Top10_Ratio"] = 0.0

        # --- NEW: VPIN (Volume-Synchronized Probability of Informed Trading) ---
        # A simplified approximation of VPIN for real-time. Full VPIN is complex.
        # This proxy measures order flow toxicity / imbalance within recent volume buckets.
        if "volume_bar" in micro_bar_data_snapshot and len(micro_bar_data_snapshot["volume_bar"]) >= 50:
            recent_volume_bars_for_vpin = micro_bar_data_snapshot["volume_bar"][-50:]
            
            sum_absolute_imbalance = 0.0
            sum_total_volume = 0.0
            for bar in recent_volume_bars_for_vpin:
                imbalance = bar['buy_volume_usd'] - bar['sell_volume_usd']
                total_bar_vol = bar['buy_volume_usd'] + bar['sell_volume_usd']
                sum_absolute_imbalance += abs(imbalance)
                sum_total_volume += total_bar_vol

            # Simplified VPIN: Sum of |Buy Vol - Sell Vol| / Sum of Total Vol
            features_dict["VPIN_50_volume_buckets"] = sum_absolute_imbalance / sum_total_volume if sum_total_volume > 0 else 0.0
        else: features_dict["VPIN_50_volume_buckets"] = 0.0

        # --- NEW: High-Frequency Volatility (Garman-Klass) ---
        # Estimates volatility using Open, High, Low, Close prices within each 5m candle.
        if "5m" in ohlcv_data_snapshot and not ohlcv_data_snapshot["5m"].empty and len(ohlcv_data_snapshot["5m"]) >= 20:
            df_5m_gk = ohlcv_data_snapshot["5m"].iloc[-20:].copy() 
            high = df_5m_gk['high'].values
            low = df_5m_gk['low'].values
            open_px = df_5m_gk['open'].values
            close_px = df_5m_gk['close'].values

            # Ensure no division by zero for log
            open_px[open_px == 0] = np.nan # Replace 0 with NaN
            low[low == 0] = np.nan
            close_px[close_px == 0] = np.nan
            
            # Garman-Klass formula: sqrt(0.5 * (ln(high/low))^2 - (2*ln(2)-1)*(ln(close/open))^2)
            log_hl = np.log(high / low)
            log_co = np.log(close_px / open_px)
            
            gk_vol_squared = 0.5 * (log_hl**2) - (2 * np.log(2) - 1) * (log_co**2)
            gk_vol_squared[gk_vol_squared < 0] = 0 # Ensure non-negative for sqrt
            gk_vol = np.sqrt(gk_vol_squared)
            
            # Return annualized volatility (e.g., 252 trading days * N 5m candles per day)
            # This needs careful scaling. Here, just the instantaneous value.
            features_dict["Garman_Klass_Vol_5m"] = gk_vol[-1] if not np.isnan(gk_vol[-1]) else 0.0
        else: features_dict["Garman_Klass_Vol_5m"] = 0.0
        
        # --- NEW: Order Book Liquidity Slope ---
        # Measures how quickly volume increases/decreases as you move away from the mid-price in the order book.
        # A steeper slope means more liquidity, less slippage.
        if order_book.get("bids") and order_book.get("asks") and len(order_book["bids"]) > 5 and len(order_book["asks"]) > 5:
            sorted_bids = sorted(order_book["bids"].items(), reverse=True) # Highest bid first
            sorted_asks = sorted(order_book["asks"].items()) # Lowest ask first

            depth_levels = 10 # Consider top 10 levels for slope calculation
            
            # Bids: volumes vs price distances from best bid (positive distance means lower price)
            best_bid_price = sorted_bids[0][0]
            bid_prices_data = np.array([p for p, q in sorted_bids[:depth_levels]])
            bid_volumes_data = np.array([q for p, q in sorted_bids[:depth_levels]])
            
            # Avoid division by zero, ensure non-zero prices
            if best_bid_price > 0 and len(bid_prices_data) > 1:
                bid_price_distances = (best_bid_price - bid_prices_data) / best_bid_price # Normalize distances
                # Filter out zero distances (best bid itself) or invalid values
                valid_indices_bids = (bid_price_distances > 1e-9) & (bid_volumes_data > 0)
                if np.sum(valid_indices_bids) >= 2: # Need at least two valid points for slope
                    bid_slope, _ = np.polyfit(bid_price_distances[valid_indices_bids], bid_volumes_data[valid_indices_bids], 1)
                else: bid_slope = 0.0
            else: bid_slope = 0.0

            # Asks: volumes vs price distances from best ask (positive distance means higher price)
            best_ask_price = sorted_asks[0][0]
            ask_prices_data = np.array([p for p, q in sorted_asks[:depth_levels]])
            ask_volumes_data = np.array([q for p, q in sorted_asks[:depth_levels]])

            if best_ask_price > 0 and len(ask_prices_data) > 1:
                ask_price_distances = (ask_prices_data - best_ask_price) / best_ask_price
                valid_indices_asks = (ask_price_distances > 1e-9) & (ask_volumes_data > 0)
                if np.sum(valid_indices_asks) >= 2:
                    ask_slope, _ = np.polyfit(ask_price_distances[valid_indices_asks], ask_volumes_data[valid_indices_asks], 1)
                else: ask_slope = 0.0
            else: ask_slope = 0.0

            # Average the slopes for overall liquidity indication
            features_dict["Orderbook_Liquidity_Slope"] = (bid_slope + ask_slope) / 2 if (bid_slope != 0 or ask_slope != 0) else 0.0
        else: features_dict["Orderbook_Liquidity_Slope"] = 0.0

        # --- NEW: Market Microprice Deviation ---
        # Microprice reflects true mid-point more accurately by considering order book liquidity.
        # Deviation from simple mid-price can indicate order flow pressure.
        if order_book.get("bids") and order_book.get("asks") and len(order_book["bids"]) > 0 and len(order_book["asks"]) > 0:
            best_bid = max(order_book["bids"].keys())
            best_ask = min(order_book["asks"].keys())
            bid_qty = order_book["bids"][best_bid]
            ask_qty = order_book["asks"][best_ask]

            if bid_qty + ask_qty > 1e-9: # Avoid division by zero
                micro_price = (best_bid * ask_qty + best_ask * bid_qty) / (bid_qty + ask_qty)
                mid_price = (best_bid + best_ask) / 2
                features_dict["Market_Microprice_Deviation_1s"] = (micro_price - mid_price) / mid_price if mid_price > 1e-9 else 0.0
            else: features_dict["Market_Microprice_Deviation_1s"] = 0.0
        else: features_dict["Market_Microprice_Deviation_1s"] = 0.0
        
        # --- NEW: Trade Intensity Ratio (1m) ---
        # Ratio of aggressive buy volume to aggressive sell volume within a time window (e.g., 1 minute).
        if len(tick_data_deque) > 0:
            # Filter ticks received within the last minute (adjust for current_time_ms)
            one_min_ago_ms = current_time_ms - 60000 
            recent_ticks = [t for t in tick_data_deque if t['timestamp'] >= one_min_ago_ms]
            
            buy_volume_usd_recent = sum(t['volume'] * t['price'] for t in recent_ticks if t['is_buyer_maker'])
            sell_volume_usd_recent = sum(t['volume'] * t['price'] for t in recent_ticks if not t['is_buyer_maker'])
            
            total_trade_volume_usd_recent = buy_volume_usd_recent + sell_volume_usd_recent
            if total_trade_volume_usd_recent > 0:
                features_dict["Trade_Intensity_Ratio_1m"] = (buy_volume_usd_recent - sell_volume_usd_recent) / total_trade_volume_usd_recent
            else: features_dict["Trade_Intensity_Ratio_1m"] = 0.0
        else: features_dict["Trade_Intensity_Ratio_1m"] = 0.0


        # --- Adaptive Normalization of Features (Couche 4: Normalisation Adaptative) ---
        # This is a conceptual implementation. For a true adaptive normalization, you'd store a rolling window of
        # each feature's values, then calculate mean and std dev dynamically.
        # [Refonte: Normalisation Adaptative des Features]
        for f_name in self.research_config["ml_features_to_use"]:
            value = features_dict.get(f_name, 0.0)
            self.rolling_feature_stats[symbol][f_name].append(value)
            # Keep only a certain number of recent values for rolling stats (e.g., last 24 hours of 1-min features = 1440 entries)
            max_rolling_values = 1440 # Example: 24 hours of 1-minute features
            while len(self.rolling_feature_stats[symbol][f_name]) > max_rolling_values:
                self.rolling_feature_stats[symbol][f_name].popleft()
            
            # Calculate rolling Z-score for the feature
            if len(self.rolling_feature_stats[symbol][f_name]) >= 30: # Need enough data for meaningful std dev
                recent_values = np.array(list(self.rolling_feature_stats[symbol][f_name]))
                mean_val = np.mean(recent_values)
                std_dev_val = np.std(recent_values)
                
                if std_dev_val > 1e-9: # Avoid division by zero
                    features_dict[f"{f_name}_Zscore_Adaptive"] = (value - mean_val) / std_dev_val
                else:
                    features_dict[f"{f_name}_Zscore_Adaptive"] = 0.0
            else:
                features_dict[f"{f_name}_Zscore_Adaptive"] = 0.0 # Not enough data for adaptive Z-score yet

        # --- Final Scaling and Output ---
        # Apply the global feature scaler (trained offline) as the final step if available.
        # The adaptive Z-scores can be used as features themselves, or you can scale them globally too.
        # The `ml_features_to_use` config should list which version (raw or Z-score) is desired.
        selected_features_names = self.research_config["ml_features_to_use"]
        
        # Create a dictionary of features ensuring all `ml_features_to_use` are present,
        # using the calculated value or 0.0 if not found.
        final_features_output = {}
        for f_name in selected_features_names:
            if f_name in features_dict:
                final_features_output[f_name] = features_dict[f_name]
            elif f"{f_name}_Zscore_Adaptive" in features_dict: # Prefer Z-score if available and configured
                 final_features_output[f_name] = features_dict[f"{f_name}_Zscore_Adaptive"]
            else:
                final_features_output[f_name] = 0.0 # Default if feature not calculated

        # Convert to numpy array for scaling
        final_features_values_array = np.array([final_features_output[f] for f in selected_features_names]).reshape(1, -1)
        
        if self.feature_scaler:
            try:
                # Transform the features using the loaded scaler
                scaled_values = self.feature_scaler.transform(final_features_values_array)[0]
                return {k: v for k, v in zip(selected_features_names, scaled_values)}
            except Exception as e:
                LOGGER.error(f"FE: Error applying global feature scaler for {symbol}: {e}. Returning unscaled features.", exc_info=True)
                return final_features_output # Return unscaled if scaling fails
        
        return final_features_output # Return unscaled if no scaler is loaded

    async def stop(self):
        """Gracefully stops the FeatureEngine and its associated tasks."""
        self._running = False
        LOGGER.info("FeatureEngine stop requested. Cancelling data processing and feature calculation tasks.")
        
        # Cancel all running tasks
        for task in self.raw_data_processing_tasks:
            task.cancel()
        for task in self.feature_calculation_tasks.values():
            task.cancel()
        
        # Signal downstream queues to stop by putting None (if applicable)
        for symbol in self.active_symbols:
            # self.tick_queues[symbol].put_nowait(None) # These are consumed by FE, not FE outputs
            # self.orderbook_queues[symbol].put_nowait(None) # These are consumed by FE, not FE outputs
            pass # FeatureEngine doesn't put sentinels, its inputs come from MDG

        # Wait for all tasks to acknowledge cancellation
        try:
            await asyncio.gather(
                *self.raw_data_processing_tasks,
                *list(self.feature_calculation_tasks.values()),
                return_exceptions=True
            )
        except asyncio.CancelledError:
            LOGGER.info("FeatureEngine: All tasks cancelled successfully during stop.")
        except Exception as e:
            LOGGER.error(f"FeatureEngine: Error during graceful stop: {e}", exc_info=True)


# --- AlphaModel (The Hypothesis Generator) ---
class AlphaModel:
    """
    AlphaModel: An ensemble of snipers, each seeking a specific opportunity,
    guided by a market regime detector. It predicts not just a signal, but a
    distribution of future returns.
    [Refonte: Ensemble Learning, Prédiction de Distribution]
    """
    def __init__(self, event_queue: asyncio.Queue, config: Dict):
        self.event_queue = event_queue
        self.config = config
        self.research_config = config["research"]
        self.ml_features_to_use = self.research_config["ml_features_to_use"]
        self.confidence_threshold = self.research_config["ml_model_confidence_threshold"]
        self.last_signal_time = defaultdict(float) # {symbol: timestamp}
        self.signal_cooldown_seconds = 5 # Minimum interval between signals for the same pair
        
        self.main_ml_model: Optional[lgb.LGBMClassifier] = None # The primary predictive model
        self.market_regime_model: Optional[Any] = None # For market regime classification (e.g., GMM, HMM)
        self.alpha_sub_models: Dict[str, Any] = {} # Dictionary of specialized alpha models (e.g., Momentum, Mean-Reversion)
        
        self.load_models() # Loads all ML models (main, regime, sub-models)
        
        # Define market regimes and their associated strategies (conceptual mapping)
        self.market_regime_strategies = {
            "BULL_TREND": ["MainModel", "MomentumHunter"],
            "BEAR_TREND": ["MainModel", "TrendFollowerShort"],
            "LOW_VOL_RANGE": ["MainModel", "MeanReversion"],
            "HIGH_VOL_CHOP": ["Scalping"], # In chop, maybe only very fast strategies
            "CRITICAL_VOLATILITY": [], # In extreme volatility, disable all trading
            "UNKNOWN_RANGE": ["MainModel"], # Default to main model if regime uncertain
        }
        LOGGER.info("AlphaModel initialized.")

    def load_models(self):
        """
        Loads all machine learning models (main alpha, market regime, sub-models)
        from their respective paths.
        [Refonte: Gestion de multiples modèles]
        """
        # Load Main Alpha Model
        main_model_path = os.path.join(self.research_config["model_save_dir"], f"{self.research_config['ml_model_save_prefix']}_v1.joblib")
        if os.path.exists(main_model_path):
            try:
                self.main_ml_model = joblib.load(main_model_path)
                LOGGER.info(f"AlphaModel: Main ML model loaded from {main_model_path}.")
            except Exception as e:
                LOGGER.error(f"AlphaModel: Error loading main ML model from {main_model_path}: {e}. Main signals will be disabled.", exc_info=True)
        else:
            LOGGER.warning(f"AlphaModel: Main ML model not found at {main_model_path}. Main signals will be disabled.")

        # Load Market Regime Model (Conceptual - typically a clustering or classification model)
        regime_model_path = os.path.join(self.research_config["model_save_dir"], "market_regime_classifier.joblib")
        if os.path.exists(regime_model_path):
            try:
                self.market_regime_model = joblib.load(regime_model_path)
                LOGGER.info(f"AlphaModel: Market regime model loaded from {regime_model_path}.")
            except Exception as e:
                LOGGER.warning(f"AlphaModel: Error loading market regime model: {e}. Falling back to rule-based regime detection.", exc_info=True)
                self.market_regime_model = None # Fallback
        else:
            LOGGER.warning(f"AlphaModel: Market regime model not found. Falling back to rule-based regime detection.")

        # Load specialized Alpha Sub-Models (e.g., for ensemble learning)
        # These are examples; actual paths and model types would vary.
        # self.alpha_sub_models["MomentumHunter"] = try_load_model(os.path.join(self.research_config["model_save_dir"], "momentum_hunter_model.joblib"))
        # self.alpha_sub_models["MeanReversion"] = try_load_model(os.path.join(self.research_config["model_save_dir"], "mean_reversion_model.joblib"))
        # self.alpha_sub_models["Scalping"] = try_load_model(os.path.join(self.research_config["model_save_dir"], "scalping_model.joblib"))
        # self.alpha_sub_models["TrendFollowerShort"] = try_load_model(os.path.join(self.research_config["model_save_dir"], "trend_follower_short_model.joblib"))
        LOGGER.info("AlphaModel: All models loading attempts complete.")


    async def on_feature_event(self, event: FeatureEvent):
        """
        Reacts to a FeatureEvent by first detecting the market regime,
        then evaluating relevant alpha models, and finally generating a trading signal.
        [Refonte: Orchestration des modèles]
        """
        symbol = event.symbol
        features_dict = event.features
        signal_timestamp_ms = event.timestamp_ms
        
        # Global Kill Switch Check (safety first)
        async with STATE_LOCK:
            if SHARED_STATE["kill_switch_active"]:
                LOGGER.debug(f"AlphaModel: Signal generation for {symbol} skipped. Global Kill Switch is ACTIVE.")
                return

        # Cooldown check: Prevent signal spamming for the same symbol
        if signal_timestamp_ms / 1000 - self.last_signal_time[symbol] < self.signal_cooldown_seconds:
            LOGGER.debug(f"AlphaModel: Signal generation for {symbol} on cooldown. Skipping.")
            return

        # 1. Market Regime Detection
        current_regime = await self._detect_market_regime(symbol, features_dict)
        async with STATE_LOCK:
            SHARED_STATE["market_regime"][symbol] = current_regime
            SHARED_STATE["telemetry_data"][f"market_regime_{symbol.replace('/','')}"] = current_regime
        LOGGER.debug(f"AlphaModel: Detected market regime for {symbol}: '{current_regime}'.")

        # 2. Prepare features for prediction
        # Ensure that all features required by ML models are present, fill with 0 if missing.
        prepared_features = np.array([features_dict.get(f, 0.0) for f in self.ml_features_to_use]).reshape(1, -1)
        
        # Basic shape validation
        if self.main_ml_model and hasattr(self.main_ml_model, 'n_features_in_') and self.main_ml_model.n_features_in_ != prepared_features.shape[1]:
            LOGGER.error(f"AlphaModel: Feature count mismatch for main model for {symbol}. Expected {self.main_ml_model.n_features_in_} features, got {prepared_features.shape[1]}. Signal ignored.")
            return
        
        # 3. Ensemble Learning / Regime-Specific Model Evaluation
        # [Refonte: Ensemble Learning de Modèles Alpha]
        active_strategies_for_regime = self.market_regime_strategies.get(current_regime, ["MainModel"])
        
        # Initialize with main model's prediction if available
        base_side = "HOLD"
        base_confidence = 0.0
        predicted_return_distribution = {"mean_return": 0.0, "std_dev_return": 0.0, "skewness": 0.0, "kurtosis": 0.0} # More detailed distribution

        if "MainModel" in active_strategies_for_regime and self.main_ml_model:
            try:
                probabilities = self.main_ml_model.predict_proba(prepared_features)[0]
                buy_prob = probabilities[1]
                sell_prob = probabilities[0]
                
                # Simulate a richer return distribution based on probabilities
                # In a real system, the ML model would directly output these (e.g., quantile regression, Mixture Density Networks)
                predicted_return_distribution["mean_return"] = (buy_prob - sell_prob) * 0.005 # Example: +/- 0.5% implied mean return
                predicted_return_distribution["std_dev_return"] = 0.01 + 0.01 * (1 - abs(buy_prob - sell_prob)) # Higher uncertainty for lower conviction
                predicted_return_distribution["skewness"] = (buy_prob - sell_prob) * 0.5 # Positive skew for buy, negative for sell
                predicted_return_distribution["kurtosis"] = 1.0 + 2.0 * (1 - abs(buy_prob - sell_prob)) # Higher kurtosis for lower conviction

                if buy_prob > self.confidence_threshold and buy_prob > sell_prob:
                    base_side = "BUY"
                    base_confidence = buy_prob
                elif sell_prob > self.confidence_threshold and sell_prob > buy_prob:
                    base_side = "SELL"
                    base_confidence = sell_prob

            except Exception as e:
                LOGGER.error(f"AlphaModel: Error predicting with main ML model for {symbol}: {e}", exc_info=True)
                # Keep base_side as HOLD if prediction fails

        # Combine signals from sub-models (conceptual: weighting, voting, or overriding logic)
        final_side = base_side
        final_confidence = base_confidence
        
        # for sub_model_name in active_strategies_for_regime:
        #     if sub_model_name != "MainModel" and sub_model_name in self.alpha_sub_models:
        #         sub_model = self.alpha_sub_models[sub_model_name]
        #         try:
        #             sub_signal, sub_conviction = sub_model.predict(prepared_features)
        #             if sub_signal == final_side and sub_conviction > 0.6:
        #                 final_confidence = min(1.0, final_confidence + sub_conviction * 0.1) # Boost confidence
        #                 LOGGER.debug(f"AlphaModel: {sub_model_name} reinforced {final_side} for {symbol}.")
        #             elif sub_signal != final_side and sub_conviction > 0.7:
        #                 LOGGER.warning(f"AlphaModel: {sub_model_name} contradicts main model for {symbol}. Signal might be weakened or ignored.")
        #                 # Example: if strong contradiction, revert to HOLD or reduce confidence
        #                 # final_side = "HOLD"
        #                 # final_confidence = 0.0
        #                 pass
        #         except Exception as e:
        #             LOGGER.warning(f"AlphaModel: Error with sub-model {sub_model_name}: {e}", exc_info=True)


        # 4. Final Signal Emission
        if final_side != "HOLD" and final_confidence > self.confidence_threshold:
            async with STATE_LOCK:
                # Get current price from latest 1s candle or order book for trigger_price (critical for signal value)
                current_ohlcv_1s_df = SHARED_STATE["ohlcv_data"][symbol].get("1s")
                current_ob = SHARED_STATE["order_book_snapshots"][symbol]
            
            trigger_price = 0.0
            if current_ohlcv_1s_df is not None and not current_ohlcv_1s_df.empty:
                trigger_price = current_ohlcv_1s_df['close'].iloc[-1]
            elif current_ob.get("bids") and current_ob.get("asks"):
                # Use mid-price from current order book
                best_bid = max(current_ob["bids"].keys())
                best_ask = min(current_ob["asks"].keys())
                trigger_price = (best_bid + best_ask) / 2
            
            if trigger_price <= 0.0:
                LOGGER.warning(f"AlphaModel: Cannot get valid trigger_price for {symbol}. Signal ignored.")
                return

            LOGGER.info(f"AlphaModel: SIGNAL {final_side} for {symbol} (Confidence: {final_confidence:.4f}, Regime: {current_regime}). Trigger Price: {trigger_price:.4f}. Pred. Return: {predicted_return_distribution['mean_return']:.4f}")
            
            # Emit SignalEvent with rich information, including predicted return distribution
            await self.event_queue.put(SignalEvent(
                symbol, final_side, final_confidence, "ML_Alpha_Model", # Use a generic strategy_id for now or combine sub-models
                trigger_price, features_dict, signal_timestamp_ms,
                predicted_return_distribution=predicted_return_distribution # Pass the predicted distribution
            ))
            self.last_signal_time[symbol] = signal_timestamp_ms / 1000 # Update cooldown timer

    async def _detect_market_regime(self, symbol: str, features: Dict) -> str:
        """
        Detects the current market regime using either a loaded ML model or a rule-based system.
        [Refonte: Modèle de Détection de Régime de Marché]
        """
        # If an ML-based market regime model is loaded, use it
        if self.market_regime_model:
            try:
                # Prepare features for the regime model (might be a different set of features)
                # For simplicity, using a subset of general features here.
                regime_features = np.array([
                    features.get("Volatility_GARCH_Predicted_1h", 0),
                    features.get("Momentum_60m_slope", 0),
                    features.get("BB_Width_1m_Pct", 0),
                    features.get("Orderbook_Liquidity_Slope", 0) # New feature
                ]).reshape(1, -1)
                
                # Predict the regime (assuming a classification model)
                predicted_regime_label = self.market_regime_model.predict(regime_features)[0]
                return predicted_regime_label # The model directly returns "BULL_TREND", "LOW_VOL_RANGE", etc.
            except Exception as e:
                LOGGER.warning(f"AlphaModel: Error predicting market regime with ML model for {symbol}: {e}. Falling back to rule-based.", exc_info=True)
                # Fallback to rule-based if ML model fails
                return await self._rule_based_market_regime_detection(symbol, features)
        else:
            # Fallback to rule-based detection if no ML model is loaded or it fails
            return await self._rule_based_market_regime_detection(symbol, features)

    async def _rule_based_market_regime_detection(self, symbol: str, features: Dict) -> str:
        """
        A rule-based fallback for market regime detection.
        Uses a combination of volatility and trend features.
        """
        volatility = features.get("Volatility_GARCH_Predicted_1h", 0.0) # From FeatureEngine
        momentum_60m = features.get("Momentum_60m_slope", 0.0) # From FeatureEngine
        
        # Get 1-day OHLCV for longer-term trend and volatility insights
        async with STATE_LOCK:
            ohlcv_1d_df = SHARED_STATE["ohlcv_data"][symbol].get("1d")
        
        long_term_trend_pct = 0.0
        if ohlcv_1d_df is not None and not ohlcv_1d_df.empty and len(ohlcv_1d_df) > 20: # Use last 20 days
            if ohlcv_1d_df['close'].iloc[-20] > 0:
                long_term_trend_pct = (ohlcv_1d_df['close'].iloc[-1] - ohlcv_1d_df['close'].iloc[-20]) / ohlcv_1d_df['close'].iloc[-20]
            
        # Define thresholds for regimes (these would be tuned/optimized)
        vol_high_threshold = self.config["risk_management"]["garch_volatility_threshold_high"] * 1.5 # Adjusted for broader context
        vol_low_threshold = self.config["risk_management"]["garch_volatility_threshold_high"] * 0.5
        trend_strong_threshold = 0.002 # 0.2% change over a period indicative of trend
        trend_weak_threshold = 0.0005 # Less than 0.05% change for ranging

        if volatility > vol_high_threshold and abs(momentum_60m) < 0.001: # High volatility but no clear direction
            return "HIGH_VOL_CHOP"
        elif volatility < vol_low_threshold: # Very low volatility
            return "LOW_VOL_RANGE"
        
        # Trend detection based on a combination of short and long-term momentum
        if momentum_60m > trend_strong_threshold or long_term_trend_pct > trend_strong_threshold * 5:
            return "BULL_TREND"
        elif momentum_60m < -trend_strong_threshold or long_term_trend_pct < -trend_strong_threshold * 5:
            return "BEAR_TREND"
        
        # If none of the above, it's a general ranging market
        if abs(momentum_60m) < trend_weak_threshold:
            return "UNKNOWN_RANGE" # Could be "MEDIUM_VOL_RANGE"
        
        return "UNKNOWN_RANGE" # Default fallback


    async def stop(self):
        LOGGER.info("AlphaModel stopped.")

# --- NEW: PositionSizer (Helper Class for PortfolioRiskManager) ---
class PositionSizer:
    """
    PositionSizer: A dedicated class for calculating optimal position sizes
    based on various methodologies (Fixed Risk, Kelly Criterion, Risk Parity).
    It ensures that capital allocation aligns with the bot's risk policy.
    [Refonte: Classe dédiée PositionSizer]
    """
    def __init__(self, config: Dict):
        self.config = config
        self.risk_config = config["risk_management"]
        self.trading_config = config["trading"]
        self.logger = LOGGER

    def calculate_position_size(self, symbol: str, side: str, trigger_price: float, initial_sl_price: float,
                                current_capital: float, current_available_capital: float,
                                confidence: float, predicted_return_distribution: Dict,
                                strategy_id: str, exchange_client: Any) -> Tuple[float, float, str]:
        """
        Calculates the optimal position size in base currency and estimated margin required.
        Uses different sizing methods based on configuration and signal characteristics.
        Returns (adjusted_amount_base_currency, estimated_margin_usd, sizing_method_used).
        [Refonte: Logique de dimensionnement avancée]
        """
        if current_capital <= 0 or current_available_capital <= 0:
            self.logger.error("PositionSizer: Insufficient capital to calculate position size (capital <= 0).")
            return 0.0, 0.0, "NO_CAPITAL"
        
        # Determine leverage for the symbol
        leverage = self.trading_config["leverage_overrides"].get(symbol, self.trading_config["leverage"])
        leverage = max(1, int(leverage)) # Ensure minimum 1x leverage

        risk_per_unit = abs(trigger_price - initial_sl_price)
        if risk_per_unit <= CONFIG["risk_management"]["min_price_tick_distance"]:
            self.logger.warning(f"PositionSizer: Calculated risk per unit is too small ({risk_per_unit:.8f}) for {symbol}. Cannot size position.")
            return 0.0, 0.0, "ZERO_RISK_PER_UNIT"

        # 1. Calculate Risk Amount in USD (Dynamic Risk Management)
        # [Refonte: Risque dynamique par trade]
        base_risk_pct = self.risk_config["per_trade_max_loss_pct_of_capital"]
        
        # Adjust risk based on confidence (Kelly-like)
        # Higher confidence -> potentially higher risk, up to a limit
        confidence_factor = (confidence - 0.5) * 2 # Scales confidence from -1 (min_conf) to 1 (max_conf)
        risk_from_confidence = base_risk_pct * (1 + confidence_factor * self.risk_config["fractional_kelly_ratio"])

        # Adjust risk based on predicted volatility (from AlphaModel's return distribution)
        # Higher predicted std_dev (more volatile) -> reduce risk
        predicted_volatility = predicted_return_distribution.get("std_dev_return", 0)
        # Simple non-linear reduction: if volatility is high, apply a multiplier
        if predicted_volatility > self.risk_config["garch_volatility_threshold_high"]:
            risk_from_vol = risk_from_confidence * self.risk_config["garch_volatility_risk_multiplier_high"]
            self.logger.info(f"PositionSizer: High predicted volatility ({predicted_volatility:.4f}) for {symbol}. Risk reduced.")
        else:
            risk_from_vol = risk_from_confidence

        # Adjust risk based on strategy's recent performance (e.g., reduce after consecutive losses)
        with STATE_LOCK:
            strat_perf = SHARED_STATE["strategy_performance_metrics"].get(strategy_id, {})
        consecutive_losses = strat_perf.get("consecutive_losses", 0)
        if consecutive_losses > 0:
            loss_penalty_factor = max(0.2, 1 - (consecutive_losses * 0.1)) # More losses = steeper reduction
            risk_amount_pct = risk_from_vol * loss_penalty_factor
            self.logger.warning(f"PositionSizer: Strategy {strategy_id} has {consecutive_losses} consecutive losses. Risk further reduced to {risk_amount_pct:.2f}%.")
        else:
            risk_amount_pct = risk_from_vol

        # Clamp the final risk percentage within min/max bounds
        final_risk_pct_of_capital = max(self.risk_config["dynamic_risk_per_trade_min_pct"], 
                                         min(self.risk_config["dynamic_risk_per_trade_max_pct"], risk_amount_pct))
        
        risk_amount_usd = current_capital * (final_risk_pct_of_capital / 100.0)

        # 2. Calculate Position Size in Base Currency (e.g., BTC amount)
        position_size_base_raw = risk_amount_usd / risk_per_unit
        
        # Adjust for exchange precision and minimum trade sizes
        adjusted_position_size = adjust_amount_for_order(symbol, position_size_base_raw, exchange_client)
        
        # Check against exchange minimum notional value (amount * price)
        market_info = exchange_client.market(symbol)
        min_notional = to_float(market_info.get('limits', {}).get('cost', {}).get('min', 0.0))
        
        if adjusted_position_size * trigger_price < min_notional * CONFIG["exchange"]["min_notional_multiplier"]:
            self.logger.warning(f"PositionSizer: Calculated position size {adjusted_position_size:.8f} for {symbol} results in notional value ({adjusted_position_size*trigger_price:.2f}) below minimum notional value {min_notional:.4f}. Size adjusted to min allowed or return 0.")
            # Option 1: Adjust to minimum allowed notional (may violate risk, but allows trade)
            # min_amount = min_notional / trigger_price
            # adjusted_position_size = adjust_amount_for_order(symbol, min_amount, exchange_client)
            # Option 2: Reject trade
            return 0.0, 0.0, "BELOW_MIN_NOTIONAL"

        # 3. Calculate Estimated Margin Required
        # Margin = (Position Size * Entry Price) / Leverage
        estimated_notional_usd = adjusted_position_size * trigger_price
        estimated_margin_usd = estimated_notional_usd / leverage

        # Final checks before returning
        if estimated_margin_usd > current_available_capital:
            self.logger.warning(f"PositionSizer: Estimated margin ({estimated_margin_usd:.2f} USDT) exceeds available capital ({current_available_capital:.2f} USDT). Scaling down to available capital.")
            # Scale down to available capital
            adjusted_position_size = (current_available_capital * leverage) / trigger_price
            adjusted_position_size = adjust_amount_for_order(symbol, adjusted_position_size, exchange_client)
            estimated_margin_usd = (adjusted_position_size * trigger_price) / leverage
            if estimated_margin_usd < min_notional * CONFIG["exchange"]["min_notional_multiplier"]: # If scaling down makes it too small
                self.logger.error(f"PositionSizer: Scaled-down position for {symbol} ({adjusted_position_size:.8f}) is too small after accounting for available capital.")
                return 0.0, 0.0, "INSUFFICIENT_AVAILABLE_CAPITAL"
            return adjusted_position_size, estimated_margin_usd, "SCALED_TO_AVAILABLE_CAPITAL"
        
        return adjusted_position_size, estimated_margin_usd, "DYNAMIC_RISK_SIZING"

    # [Refonte: Risk Parity Sizing (Conceptual)]
    def _calculate_risk_parity_size(self, symbol: str, current_capital: float, predicted_annualized_vol: float,
                                    portfolio_target_vol_annualized_pct: float) -> float:
        """
        (Conceptual) Calculates position size based on Risk Parity principles.
        Attempts to allocate capital such that each position contributes equally to overall portfolio risk.
        Requires good volatility estimates (predicted_annualized_vol for the symbol).
        """
        if predicted_annualized_vol <= 0: return 0.0 # Cannot size if no volatility

        # Target portfolio volatility (e.g., 20% annualized)
        target_portfolio_vol_decimal = portfolio_target_vol_annualized_pct / 100.0

        # Number of positions (simplified: assumes target for this single position)
        # In a full Risk Parity, you'd consider all active and potential positions and their correlations.
        
        # Contribution of this asset to portfolio volatility
        # Simplified: target each asset to contribute `target_portfolio_vol / sqrt(N_assets)`
        # This is a very basic form and ignores correlation.
        
        # In a more advanced setup:
        # Asset Risk Contribution = Weight * Volatility * Correlation_to_Portfolio
        # For simplicity, if we want this asset to contribute a certain amount of risk,
        # we target its volatility as a fraction of portfolio target volatility.

        # For a single asset, if we aim for X% portfolio vol:
        # Amount_in_USD * Asset_Volatility = Target_Portfolio_Volatility * Total_Capital
        # Amount_in_USD = (Target_Portfolio_Volatility / Asset_Volatility) * Total_Capital
        
        # Simplified: target percentage of capital to risk based on its vol vs target vol
        # This needs careful calibration and ignores correlation effects.
        # A more robust approach would compute the marginal risk contribution
        # of adding this position to the existing portfolio.

        # Example simplistic scaling based on relative volatility:
        # If asset is very volatile, reduce size. If less volatile, increase.
        # Scale to match target portfolio volatility when considered alone.
        # This assumes the predicted_annualized_vol is in decimal form.
        
        # Desired USD exposure for this asset = (Target_Portfolio_Vol_decimal / Predicted_Asset_Vol_decimal) * Current_Capital
        # This needs to be carefully scaled down to avoid over-leveraging.
        # For a single trade, use it as a factor relative to your base risk.

        # A more practical interpretation in this context:
        # If the predicted_annualized_vol is very high, reduce the `final_risk_pct_of_capital` from earlier.
        # This is already implicitly covered in `_calculate_dynamic_risk_percentage` by `garch_volatility_risk_multiplier_high`.
        
        # For a true Risk Parity, one would need the covariance matrix of all assets in the portfolio
        # and optimize weights such that each asset contributes equally to the total portfolio risk.
        # This is beyond a simple function and requires portfolio optimization solvers.

        # For this refactor, the `calculate_position_size` already includes a volatility adjustment.
        # We'll return 0.0 to indicate this is not the primary sizing method used here.
        self.logger.debug(f"PositionSizer: Risk Parity Sizing is conceptual here. Using dynamic risk percentage.")
        return 0.0


# --- PortfolioRiskManager (The Grand Strategist) ---
class PortfolioRiskManager:
    """
    PortfolioRiskManager: Moves from simple "risk management" to "optimal capital allocation".
    It not only defends against losses but also strategically decides where and how to deploy capital,
    considering portfolio-level objectives and dynamic correlations.
    [Refonte: Allocation de capital optimale, Stratégie de portefeuille]
    """
    def __init__(self, event_queue: asyncio.Queue, config: Dict):
        self.event_queue = event_queue
        self.config = config
        self.trading_config = config["trading"]
        self.risk_config = config["risk_management"]
        self.exchange_config = config["exchange"]
        self.active_symbols = self.exchange_config["active_symbols"]

        self._running = False
        self.exchange_client: Optional[ccxt.Exchange] = None # For REST API calls (balances, positions)
        self.ccxt_config = {
            'apiKey': BINANCE_API_KEY,
            'secret': BINANCE_API_SECRET,
            'options': {
                'defaultType': 'future' if self.trading_config["leverage"] > 1 else 'spot',
                'adjustForTimeDifference': True, # <--- AJOUTEZ CETTE LIGNE
            }
        }
        if self.exchange_config["sandbox_mode"]:
            # Binance Futures Testnet URL
            self.ccxt_config['urls'] = {'api': 'https://testnet.binancefuture.com/fapi'}
            LOGGER.warning("PortfolioRiskManager: Sandbox mode ACTIVE. Connecting to Binance Futures testnet.")
        
        self.last_capital_update_time = 0.0 # For periodic balance/position fetch
        self.last_position_mgmt_time = 0.0 # For periodic SL/TP/TSL checks
        self.last_daily_reset_date = datetime.date.today()
        
        # Circuits breakers for external APIs
        self.exchange_api_circuit_breaker = CircuitBreaker(
            failure_threshold=5, recovery_timeout=60, logger=LOGGER # More lenient than trading path
        )
        
        # Instantiate the dedicated PositionSizer
        self.position_sizer = PositionSizer(config) # [Refonte: Intégration de PositionSizer]

        # Dynamic covariance matrix for portfolio risk (Conceptual for live)
        # This would be updated by FeatureEngine or a dedicated StatsEngine.
        self.dynamic_covariance_matrix: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float)) # {symbol1: {symbol2: covariance}}
        self.last_covariance_update_time = 0.0
        
        LOGGER.info("PortfolioRiskManager initialized.")

    async def run(self):
        """Starts the PortfolioRiskManager, connecting to the exchange and running its main loop."""
        self._running = True
        LOGGER.info("PortfolioRiskManager starting. Connecting to exchange for account data.")
        
        exchange_class = getattr(ccxt, self.exchange_config["name"])
        self.exchange_client = exchange_class(self.ccxt_config)
        
        if self.exchange_config["sandbox_mode"] and hasattr(self.exchange_client, 'set_sandbox_mode'):
            self.exchange_client.set_sandbox_mode(True)

        try:
            await self.exchange_api_circuit_breaker(self.exchange_client.load_markets)()
            LOGGER.info(f"PortfolioRiskManager: Connected to {self.exchange_client.name}. Markets loaded.")
            await self._update_capital_and_positions_from_exchange() # Initial sync
        except Exception as e:
            LOGGER.critical(f"PortfolioRiskManager: Failed to connect to exchange: {e}. Cannot operate.", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("CRITICAL", f"PRM: Exchange connection failed on startup. Cannot operate."))
            self._running = False
            return

        await self._main_loop()

    async def _main_loop(self):
        """
        The main loop for PortfolioRiskManager, periodically updating account state,
        managing positions (SL/TP/TSL), and checking global risk limits.
        """
        balance_fetch_interval = 60 # Fetch balances/positions every 60 seconds
        position_mgmt_interval = 5  # Check SL/TP/TSL every 5 seconds
        
        while self._running:
            await asyncio.sleep(1) # Base sleep interval
            current_time = time.time()
            
            # Reset daily metrics at the start of a new day
            today = datetime.date.today()
            if today != self.last_daily_reset_date:
                await self.reset_daily_metrics()
                self.last_daily_reset_date = today

            # Periodically update capital and positions from exchange REST API
            if current_time - self.last_capital_update_time > balance_fetch_interval:
                await self._update_capital_and_positions_from_exchange()
                self.last_capital_update_time = current_time
            
            # Periodically manage active positions (SL/TP/TSL)
            if current_time - self.last_position_mgmt_time > position_mgmt_interval:
                await self._manage_open_positions()
                self.last_position_mgmt_time = current_time

            async with STATE_LOCK:
                SHARED_STATE["telemetry_data"]["prm_loop_count"] = SHARED_STATE["telemetry_data"].get("prm_loop_count", 0) + 1

    async def reset_daily_metrics(self):
        """Resets daily PnL, fees, and other performance metrics for strategies and global capital."""
        LOGGER.info("PRM: Resetting daily metrics.")
        async with STATE_LOCK:
            for strat_id in SHARED_STATE["strategy_performance_metrics"]:
                SHARED_STATE["strategy_performance_metrics"][strat_id]["daily_pnl_usd"] = 0.0
                # Reset consecutive losses if not already at max, or if a profitable trade occurred.
                # Here, we do a simple reset to 0. A more nuanced approach would be better.
                SHARED_STATE["strategy_performance_metrics"][strat_id]["consecutive_losses"] = 0 
                SHARED_STATE["strategy_performance_metrics"][strat_id]["last_reset_date"] = datetime.date.today().isoformat()

            for sym in self.active_symbols:
                SHARED_STATE["symbol_daily_drawdown"][sym] = 0.0
            
            SHARED_STATE["global_capital"]["daily_pnl"] = 0.0
            SHARED_STATE["global_capital"]["daily_fees"] = 0.0
            SHARED_STATE["global_capital"]["daily_profit_reached"] = False # Reset daily profit target flag
        await self.event_queue.put(SystemAlertEvent("INFO", "PRM: Daily metrics reset."))

    @retry_async(max_retries=3, delay_seconds=5, catch_exceptions=(ccxt.NetworkError, ccxt.ExchangeError))
    @CircuitBreaker(failure_threshold=5, recovery_timeout=300, logger=LOGGER) # If 5 failures, circuit opens for 5 mins
    async def _update_capital_and_positions_from_exchange(self):
        """
        Updates global capital and active position states by fetching data from the exchange.
        Also checks for global drawdown and liquidation risks.
        [Refonte: Reconciliation des positions, gestion du DD, alertes de liquidation]
        """
        LOGGER.debug("PRM: Fetching latest balance and positions from exchange.")
        try:
            balance = await self.exchange_client.fetch_balance()
            positions_raw = await self.exchange_client.fetch_positions() # Futures positions

            async with STATE_LOCK:
                ccy = self.trading_config["capital_currency"]
                # Total balance usually includes wallet balance + unrealized PnL for futures
                total_balance = to_float(safe_get(balance, f"total.{ccy}", 0.0))
                free_balance = to_float(safe_get(balance, f"free.{ccy}", 0.0)) # Available for new orders/withdrawals
                
                SHARED_STATE["global_capital"]["total"] = total_balance
                SHARED_STATE["global_capital"]["available"] = free_balance
                SHARED_STATE["global_capital"]["current_equity"] = total_balance # For now, equity = total balance

                if SHARED_STATE["global_capital"]["initial"] == 0.0 and total_balance > 0:
                    SHARED_STATE["global_capital"]["initial"] = total_balance
                    SHARED_STATE["global_capital"]["peak"] = total_balance
                    LOGGER.info(f"PRM: Initial capital detected: {total_balance:.2f} {ccy}.")

                # Update peak equity and calculate max drawdown
                if total_balance > SHARED_STATE["global_capital"]["peak"]:
                    SHARED_STATE["global_capital"]["peak"] = total_balance
                
                current_drawdown_pct = ((SHARED_STATE["global_capital"]["peak"] - total_balance) / SHARED_STATE["global_capital"]["peak"]) * 100 if SHARED_STATE["global_capital"]["peak"] > 0 else 0.0
                SHARED_STATE["global_capital"]["max_drawdown_pct"] = max(SHARED_STATE["global_capital"]["max_drawdown_pct"], current_drawdown_pct)

                # Global Kill Switch Check (based on drawdown)
                if current_drawdown_pct >= self.risk_config["global_max_drawdown_pct"]:
                    if not SHARED_STATE["kill_switch_active"]:
                        LOGGER.critical(f"PRM: GLOBAL KILL SWITCH TRIGGERED: Drawdown {current_drawdown_pct:.2f}% >= {self.risk_config['global_max_drawdown_pct']}%. Initiating bot shutdown.")
                        await self.event_queue.put(SystemAlertEvent("CRITICAL", f"PRM: GLOBAL KILL SWITCH ACTIVATED. Total Drawdown: {current_drawdown_pct:.2f}%"))
                        await self.event_queue.put(BotCommandEvent("ACTIVATE_GLOBAL_KILL_SWITCH", {"reason": "MaxDrawdownExceeded"}))
                        SHARED_STATE["kill_switch_active"] = True
                
                # Daily Profit Target Check (to reduce exposure if target is met)
                if self.risk_config["daily_profit_target_pct"] > 0:
                    daily_profit_usd = SHARED_STATE["global_capital"]["daily_pnl"]
                    # Calculate percentage based on initial capital for the day, or current capital.
                    # Use initial_capital for the day (assuming reset daily) for consistency
                    daily_profit_pct = (daily_profit_usd / SHARED_STATE["global_capital"]["initial"]) * 100 if SHARED_STATE["global_capital"]["initial"] > 0 else 0
                    
                    if daily_profit_pct >= self.risk_config["daily_profit_target_pct"]:
                        if not SHARED_STATE["global_capital"]["daily_profit_reached"]:
                            LOGGER.info(f"PRM: Daily profit target ({self.risk_config['daily_profit_target_pct']:.2f}%) reached! PnL: {daily_profit_pct:.2f}%. Considering reduced activity or full stop.")
                            await self.event_queue.put(SystemAlertEvent("INFO", f"PRM: Daily Profit Target Reached! PnL: {daily_profit_pct:.2f}%"))
                            SHARED_STATE["global_capital"]["daily_profit_reached"] = True
                            # Could trigger BotCommandEvent("PAUSE_STRATEGIES_IF_PROFIT_TARGET_MET") for AlphaModel/PRM to act

                # Reconcile internal `active_positions` with exchange-reported positions
                # Filter out zero-amount positions from exchange response
                exchange_open_positions_map = {
                    pos['symbol']: pos for pos in positions_raw 
                    if to_float(pos.get('info', {}).get('positionAmt', 0)) != 0
                }
                
                # Check positions known by bot
                for internal_pos_id, internal_pos_data in list(SHARED_STATE["active_positions"].items()): # Iterate over copy
                    symbol = internal_pos_data["symbol"]
                    exchange_pos = exchange_open_positions_map.pop(symbol, None) # Remove from map if found on exchange
                    
                    if exchange_pos:
                        # Update internal position with latest data from exchange (PnL, liq price, margin)
                        internal_pos_data['pnl_usd'] = to_float(exchange_pos.get('unrealizedPnl', 0.0))
                        internal_pos_data['liquidation_price'] = to_float(exchange_pos.get('liquidationPrice', 0.0))
                        internal_pos_data['margin_used'] = to_float(exchange_pos.get('initialMargin', 0.0)) # Or `isolatedWallet` for isolated margin
                        internal_pos_data['leverage'] = to_float(exchange_pos.get('leverage', self.trading_config["leverage"]))
                        internal_pos_data['current_amount'] = abs(to_float(exchange_pos.get('positionAmt', 0.0))) # Ensure current amount reflects exchange
                        
                        # Recalculate PnL percentage based on updated margin
                        if internal_pos_data['margin_used'] > 1e-9: # Avoid division by zero
                            internal_pos_data['pnl_percentage'] = (internal_pos_data['pnl_usd'] / internal_pos_data['margin_used']) * 100
                        else:
                            internal_pos_data['pnl_percentage'] = 0.0

                        # Liquidation Risk Alert
                        current_price = self._get_current_price_from_data_cache(symbol)
                        if current_price and internal_pos_data['liquidation_price'] > 1e-9:
                            dist_to_liq_pct = abs(current_price - internal_pos_data['liquidation_price']) / current_price * 100
                            # Check if price is moving towards liquidation and within alert threshold
                            if (internal_pos_data['side'] == 'BUY' and current_price < internal_pos_data['liquidation_price'] and dist_to_liq_pct <= self.risk_config['liquidation_alert_threshold_pct']) or \
                               (internal_pos_data['side'] == 'SELL' and current_price > internal_pos_data['liquidation_price'] and dist_to_liq_pct <= self.risk_config['liquidation_alert_threshold_pct']):
                                LOGGER.critical(f"PRM: LIQUIDATION RISK for {symbol} ({internal_pos_id[:8]}): Current Price {current_price:.4f}, Liq Price {internal_pos_data['liquidation_price']:.4f} (Dist: {dist_to_liq_pct:.2f}%)")
                                await self.event_queue.put(SystemAlertEvent("CRITICAL", f"LIQUIDATION RISK {symbol} ({internal_pos_id[:8]}): Price {current_price:.4f} near Liq {internal_pos_data['liquidation_price']:.4f}!"))
                        
                        # Update highest/lowest price for Trailing Stop Loss tracking
                        if current_price:
                            if internal_pos_data['side'] == 'BUY':
                                internal_pos_data['highest_profit_price'] = max(internal_pos_data.get('highest_profit_price', internal_pos_data['entry_price']), current_price)
                            else: # SELL
                                internal_pos_data['lowest_profit_price'] = min(internal_pos_data.get('lowest_profit_price', internal_pos_data['entry_price']), current_price)
                        
                        # Check funding rates for futures positions
                        if self.exchange_config["name"] == "binanceusdm": # Specific to Binance Futures
                            try:
                                funding_info = await self.exchange_client.fetch_funding_rate(symbol)
                                funding_rate = to_float(funding_info.get('fundingRate', 0.0))
                                if funding_rate != 0:
                                    if (internal_pos_data['side'] == 'BUY' and funding_rate > self.risk_config['funding_rate_threshold_long_pct']) or \
                                       (internal_pos_data['side'] == 'SELL' and funding_rate < self.risk_config['funding_rate_threshold_short_pct']):
                                        LOGGER.warning(f"PRM: High adverse funding rate for {symbol}: {funding_rate*100:.4f}%. Position side: {internal_pos_data['side']}.")
                                        await self.event_queue.put(SystemAlertEvent("WARNING", f"High Adverse Funding: {symbol} {funding_rate*100:.4f}%"))
                            except Exception as fund_e:
                                LOGGER.warning(f"PRM: Could not fetch funding rate for {symbol}: {fund_e}")

                    else:
                        # Position in bot's state but not found on exchange. It might have been closed manually or by liquidation.
                        # This requires careful handling: fetch the specific order related to the position (if any recent)
                        # For simplicity, assume it's closed, but this could be a source of desynchronization.
                        if internal_pos_data['status'] == 'open':
                            LOGGER.warning(f"PRM: Position {symbol} ({internal_pos_id[:8]}) open in bot but not found on exchange. Assuming closed by external means (manual close or liquidation). Removing from active positions.")
                            # Trigger a "finalization" to clean up internal state
                            await self._close_position_finalize(internal_pos_data, internal_pos_data['entry_price'], "EXTERNAL_CLOSE_UNTRACKED")
                            # It's crucial to ensure all associated orders (SL/TP) are cancelled.
                            await self.event_queue.put(OrderRequestEvent(
                                request_type="CANCEL_ALL_FOR_POS", symbol=symbol, order_type="", side="", amount=0, price=0,
                                purpose="CLEANUP_EXTERNAL_CLOSE", position_id=internal_pos_id
                            ))
                
                # Any remaining positions in `exchange_open_positions_map` were opened externally and are not managed by the bot.
                for symbol_ext, exchange_pos_ext in exchange_open_positions_map.items():
                    position_amount_info = exchange_pos_ext.get('info', {}).get('positionAmt', 'N/A')
                    LOGGER.warning(f"PRM: External position detected on exchange for {symbol_ext} (Amount: {position_amount_info}). Not managed by Kronos. Consider manual review.")
                    await self.event_queue.put(SystemAlertEvent("WARNING", f"PRM: External Position detected: {symbol_ext} {exchange_pos_ext['positionAmt']}"))

                LOGGER.info(f"PRM: Capital Total: {total_balance:.2f} {ccy}, Free: {free_balance:.2f} {ccy}. Active Positions: {len(SHARED_STATE['active_positions'])}.")
                
        except CircuitBreakerOpenError:
            LOGGER.warning("PRM: Circuit Breaker is OPEN for exchange API. Skipping balance/position update.")
            await self.event_queue.put(HealthCheckEvent("ExchangeAPI", "warning", "Circuit Breaker OPEN. API calls for balance/positions skipped."))
        except Exception as e:
            LOGGER.error(f"PRM: Error updating capital/positions from exchange: {e}", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("ERROR", f"PRM: Error fetching capital/pos: {e}"))

    async def on_signal_event(self, event: SignalEvent):
        """
        Reacts to a SignalEvent from AlphaModel to calculate position size,
        check risk limits, and request an order.
        [Refonte: Calcul de taille de position, vérification des contraintes]
        """
        async with STATE_LOCK:
            # Check global kill switch BEFORE processing the signal
            if SHARED_STATE["kill_switch_active"]:
                LOGGER.warning(f"PRM: Signal {event.side} for {event.symbol} ignored. Global Kill Switch Active.")
                return
            # Check if daily profit target is met and actions are configured to pause strategies
            if SHARED_STATE["global_capital"]["daily_profit_reached"]:
                # This logic could be more nuanced (e.g., only pause new entries, allow exits)
                LOGGER.info(f"PRM: Signal {event.side} for {event.symbol} ignored. Daily profit target reached.")
                return

        symbol = event.symbol
        side = event.side
        confidence = event.confidence
        strategy_id = event.strategy_id
        trigger_price = event.trigger_price
        features_at_signal = event.features_at_signal
        signal_timestamp_ms = event.signal_timestamp_ms
        predicted_return_distribution = event.predicted_return_distribution

        async with STATE_LOCK:
            current_capital = SHARED_STATE["global_capital"]["total"]
            current_available_capital = SHARED_STATE["global_capital"]["available"]
            open_positions_count = len(SHARED_STATE["active_positions"])
            open_positions_for_symbol = sum(1 for p in SHARED_STATE["active_positions"].values() if p["symbol"] == symbol and p["status"] == "open")
            
            strategy_params = SHARED_STATE["live_strategy_parameters"].get(strategy_id, {"enabled": True})
            if not strategy_params.get("enabled", True): # Check if strategy is explicitly disabled
                LOGGER.info(f"PRM: Signal {side} {symbol} ignored. Strategy '{strategy_id}' is disabled.")
                return

        if current_capital <= 0:
            LOGGER.error("PRM: Total capital <= 0. Cannot calculate position size. Signal ignored.")
            await self.event_queue.put(SystemAlertEvent("ERROR", "PRM: Zero Capital. Cannot place trades."))
            return

        # Check concurrent positions limits
        if open_positions_count >= self.trading_config["max_concurrent_positions_total"]:
            LOGGER.info(f"PRM: Signal {side} {symbol} ignored. Max total positions ({open_positions_count}) reached.")
            return
        if open_positions_for_symbol >= self.trading_config["max_concurrent_positions_per_symbol"]:
            LOGGER.info(f"PRM: Signal {side} {symbol} ignored. Max positions per symbol ({open_positions_for_symbol}) reached.")
            return

        # Get latest OHLCV data for SL/TP calculations (e.g., 1m or 5m data)
        async with STATE_LOCK:
            ohlcv_df_for_sl = SHARED_STATE["ohlcv_data"][symbol].get("1m") # Typically use lower timeframe for SL/TP base
        
        if ohlcv_df_for_sl is None or ohlcv_df_for_sl.empty or len(ohlcv_df_for_sl) < max(self.risk_config["stop_loss_atr_period"], self.risk_config["trailing_stop_atr_period"]) + 2:
            LOGGER.warning(f"PRM: Not enough 1m OHLCV data for {symbol} for SL/TP calculation. Signal ignored.")
            await self.event_queue.put(SystemAlertEvent("WARNING", f"PRM: Insufficient OHLCV for {symbol} SL/TP. Signal ignored."))
            return
        
        # Calculate initial Stop Loss price
        initial_sl_price = await self._calculate_stop_loss(symbol, side, trigger_price, ohlcv_df_for_sl)
        if initial_sl_price is None or initial_sl_price <= 0:
            LOGGER.warning(f"PRM: Failed to define valid initial SL price for {symbol}. Signal ignored.")
            return

        # Calculate Take Profit levels
        take_profit_levels_info = await self._calculate_take_profit(symbol, side, trigger_price, initial_sl_price)
        if not take_profit_levels_info:
            LOGGER.warning(f"PRM: Failed to define valid Take Profit levels for {symbol}. Signal ignored.")
            return
        
        # Determine position size using PositionSizer
        # [Refonte: Utilisation de PositionSizer]
        adjusted_position_size, estimated_margin_usd, sizing_method = \
            self.position_sizer.calculate_position_size(
                symbol, side, trigger_price, initial_sl_price,
                current_capital, current_available_capital,
                confidence, predicted_return_distribution,
                strategy_id, self.exchange_client # Pass exchange client for precision
            )
        
# 1. Validation de la taille de position calculée
        if adjusted_position_size <= 1e-9: # Si la taille est effectivement nulle
            LOGGER.warning(f"PRM: Calculated position size is zero or too small for {symbol}. Signal ignored. Reason: {sizing_method}")
            return

        # 2. Validation de l'exposition maximale
        # Vérifier l'exposition maximale par symbole (somme de la marge utilisée pour les positions ouvertes sur ce symbole)
        async with STATE_LOCK:
            current_symbol_exposure = sum(p.get('margin_used', 0.0) for p in SHARED_STATE["active_positions"].values() if p["symbol"] == symbol and p["status"] == "open")

        max_symbol_exposure = current_capital * (self.trading_config["max_exposure_per_symbol_pct_of_equity"] / 100.0)

        if current_symbol_exposure + estimated_margin_usd > max_symbol_exposure:
            LOGGER.info(f"PRM: Signal {side} {symbol} ignored. Max exposure per symbol would be exceeded ({current_symbol_exposure + estimated_margin_usd:.2f} > {max_symbol_exposure:.2f} USDT).")
            return

        # --- Toutes les validations ont réussi, procéder à la création de la position ---

        LOGGER.info(f"PRM: Signal ACCEPTED: {side} {symbol}. Sizing method: {sizing_method}. Amount: {adjusted_position_size:.8f} @ {trigger_price:.5f}. Est. Margin: {estimated_margin_usd:.2f} USDT. SL: {initial_sl_price:.5f}.")

        # Créer un ID de position interne unique
        position_id = generate_unique_id(f"pos_{symbol.replace('/','')}")

        # Déterminer l'effet de levier pour le symbole
        leverage = self.trading_config["leverage_overrides"].get(symbol, self.trading_config["leverage"])

        # Préparer la position "pending_entry" dans SHARED_STATE
        # Ceci sera mis à jour par les OrderUpdateEvents
        async with STATE_LOCK:
            SHARED_STATE["active_positions"][position_id] = {
                "position_id": position_id,
                "symbol": symbol,
                "side": side,
                "status": "pending_entry", # Deviendra 'open' au premier remplissage
                "entry_price": trigger_price, # Prix de l'ordre initial, deviendra le prix moyen rempli
                "initial_amount": adjusted_position_size, # Montant demandé
                "current_amount": 0.0, # Le montant rempli mettra à jour ceci
                "original_amount": adjusted_position_size, # Base pour les montants TP
                "entry_time": signal_timestamp_ms / 1000, # Heure de traitement du signal (secondes)
                "initial_stop_loss": initial_sl_price,
                "current_stop_loss": initial_sl_price,
                "take_profit_levels": take_profit_levels_info,
                "current_trailing_stop_level": None,
                "highest_profit_price": trigger_price if side == "BUY" else -np.inf, # Suivi pour TSL
                "lowest_profit_price": trigger_price if side == "SELL" else np.inf, # Suivi pour TSL
                "entry_order_id": None,
                "stop_loss_order_id": None,
                "take_profit_order_ids": [],
                "strategy_id": strategy_id,
                "confidence_at_entry": confidence,
                "features_at_entry": features_at_signal,
                "predicted_return_distribution_at_entry": predicted_return_distribution,
                "leverage": leverage,
                "margin_used": estimated_margin_usd, # Marge initiale estimée
                "liquidation_price": 0.0,
                "pnl_usd": 0.0,
                "pnl_percentage": 0.0,
                "realized_pnl": 0.0,
                "total_fees": 0.0,
                "close_reason": None,
                "close_time": None,
                "duration_seconds": None,
                "last_update_timestamp": get_current_utc_ms(),
                "daily_max_drawdown_pct": 0.0, # Suivi du drawdown quotidien par symbole
            }

        # Préparer et envoyer l'événement de demande d'ordre à l'ExecutionHandler
        order_request = OrderRequestEvent(
            request_type="CREATE",
            symbol=symbol,
            order_type=self.trading_config["default_entry_order_type"],
            side=side,
            amount=adjusted_position_size,
            price=trigger_price if self.trading_config["default_entry_order_type"] == "LIMIT" else None,
            purpose=f"ENTRY_{strategy_id}",
            position_id=position_id,
            params={
                "leverage": leverage, # Définir l'effet de levier pour la transaction
                "timeInForce": self.trading_config["entry_order_time_in_force"]
            },
            algo_params={"algo_type": "MARKET_OR_LIMIT_SNIPER"} # Exemple: Indiquer à EH quel algo utiliser
        )
        await self.event_queue.put(order_request)

        # Envoyer un événement pour notifier les autres modules qu'une position est en attente
        await self.event_queue.put(PositionUpdateEvent(position_id, symbol, "PENDING_ENTRY", {"amount": adjusted_position_size, "price": trigger_price}))
    async def on_order_update_event(self, event: OrderUpdateEvent):
        """
        Processes OrderUpdateEvents from ExecutionHandler to update position state,
        calculate realized PnL, and manage strategy performance metrics.
        [Refonte: Gestion des ordres complexes]
        """
        async with STATE_LOCK:
            # Reconcile order IDs for fast lookup
            if event.client_order_id and event.exchange_order_id:
                SHARED_STATE["client_order_map"][event.client_order_id] = event.exchange_order_id
            
            # Retrieve the position associated with this order update
            position_id = event.position_id
            position = SHARED_STATE["active_positions"].get(position_id)

            if not position:
                # This can happen if an order for an already closed/untracked position comes in.
                # Or if it's an external order not related to Kronos positions.
                LOGGER.warning(f"PRM: OrderUpdateEvent for unknown/inactive position ID {position_id}. Order ID: {event.exchange_order_id}. Purpose: {event.purpose}. Ignoring for position state update.")
                return

            symbol = event.symbol
            order_status = event.status
            purpose = event.purpose
            
            # Update position status based on order fills
            if purpose.startswith("ENTRY_"):
                # Handle entry order updates
                if order_status == "filled":
                    # For simplified logic, assume single entry. For partial fills, this needs to be more complex.
                    # If multiple entries for scaling in, update average entry price and total amount.
                    if position["status"] == "pending_entry":
                        position["status"] = "open"
                        position["entry_price"] = event.fill_price # First fill price
                        position["current_amount"] = event.filled_amount
                        position["original_amount"] = event.filled_amount # Base for TP percentages
                        position["entry_order_id"] = event.exchange_order_id
                        LOGGER.info(f"PRM: Position {position_id[:8]} for {symbol} OPENED at {event.fill_price:.4f} with {event.filled_amount:.8f} (Fully filled).")
                        await self.event_queue.put(PositionUpdateEvent(position_id, symbol, "OPENED", {"price": event.fill_price, "amount": event.filled_amount}))
                        
                        # Once opened, place initial SL/TP orders
                        await self._place_initial_exit_orders(position) # [Refonte: Placement des ordres de sortie initiaux]
                    elif order_status == "partial_fill" and CONFIG["trading"]["partial_fill_reentry_enabled"]:
                        LOGGER.info(f"PRM: Position {position_id[:8]} for {symbol} PARTIALLY FILLED: {event.filled_amount:.8f} at {event.fill_price:.4f}.")
                        # Update current amount and potentially average entry price
                        new_total_amount = position["current_amount"] + event.filled_amount
                        position["entry_price"] = ((position["entry_price"] * position["current_amount"]) + (event.fill_price * event.filled_amount)) / new_total_amount
                        position["current_amount"] = new_total_amount
                        await self.event_queue.put(PositionUpdateEvent(position_id, symbol, "SCALED_IN", {"price": event.fill_price, "amount": event.filled_amount}))
                        # Decide if to cancel remaining or resubmit (depends on strategy)

                elif order_status == "canceled" and position["status"] == "pending_entry":
                    LOGGER.warning(f"PRM: Entry order for {position_id[:8]} ({symbol}) cancelled before full fill. Removing pending position.")
                    SHARED_STATE["active_positions"].pop(position_id)
                    await self.event_queue.put(PositionUpdateEvent(position_id, symbol, "ENTRY_CANCELED", {"reason": "OrderCancelled", "filled": event.filled_amount}))
                elif order_status == "rejected" and position["status"] == "pending_entry":
                    LOGGER.error(f"PRM: Entry order for {position_id[:8]} ({symbol}) REJECTED by exchange. Removing pending position. Details: {event.order_details.get('info')}")
                    SHARED_STATE["active_positions"].pop(position_id)
                    await self.event_queue.put(PositionUpdateEvent(position_id, symbol, "ENTRY_REJECTED", {"reason": "OrderRejected", "details": event.order_details.get('info')}))

            elif purpose.startswith("TAKE_PROFIT_L"):
                # Handle Take Profit order updates
                tp_level_num = int(purpose.split('L')[1])
                target_tp_level = next((lvl for lvl in position["take_profit_levels"] if lvl["level"] == tp_level_num), None)
                
                if target_tp_level and order_status == "filled":
                    LOGGER.info(f"PRM: TP Level {tp_level_num} for {position_id[:8]} ({symbol}) FILLED at {event.fill_price:.4f}. Amount: {event.filled_amount:.8f}.")
                    
                    # Update realized PnL and total fees
                    pnl_on_close = calculate_pnl_usd(position["entry_price"], event.fill_price, event.filled_amount, position["side"])
                    position["realized_pnl"] += pnl_on_close
                    position["total_fees"] += event.fee
                    position["current_amount"] -= event.filled_amount # Reduce position size
                    
                    target_tp_level["status"] = "hit"
                    target_tp_level["fill_price"] = event.fill_price
                    target_tp_level["filled_amount"] = event.filled_amount
                    
                    # Update global PnL metrics
                    SHARED_STATE["global_capital"]["lifetime_realized_pnl"] += pnl_on_close
                    SHARED_STATE["global_capital"]["lifetime_fees_paid"] += event.fee
                    SHARED_STATE["global_capital"]["daily_pnl"] += pnl_on_close
                    SHARED_STATE["global_capital"]["daily_fees"] += event.fee

                    # Update strategy performance metrics
                    strat_perf = SHARED_STATE["strategy_performance_metrics"][position["strategy_id"]]
                    strat_perf["total_pnl_usd"] += pnl_on_close
                    strat_perf["daily_pnl_usd"] += pnl_on_close
                    strat_perf["consecutive_losses"] = 0 # Break consecutive losses on profit

                    await self.event_queue.put(PositionUpdateEvent(position_id, symbol, "PARTIAL_CLOSE", {"reason": purpose, "price": event.fill_price, "amount": event.filled_amount, "pnl_usd": pnl_on_close}))
                    
                    # Check if position is fully closed (current_amount is zero or very small)
                    if position["current_amount"] <= CONFIG["exchange"]["min_notional_multiplier"] * 0.00001:
                        await self._close_position_finalize(position, event.fill_price, "TAKE_PROFIT_COMPLETE")
                        
                    # After a TP fill, evaluate moving SL to Break-Even if configured
                    if CONFIG["risk_management"]["move_sl_to_be_after_tp1"] and tp_level_num == 1 and position["status"] == "open":
                        await self._move_sl_to_break_even_logic(position) # [Refonte: SL au BE après TP1]

                elif target_tp_level and (order_status == "canceled" or order_status == "rejected"):
                    LOGGER.warning(f"PRM: TP order for level {tp_level_num} ({symbol}) cancelled/rejected. Status: {order_status}. Reason: {event.order_details.get('info')}.")
                    target_tp_level["status"] = "failed" # Mark as failed to avoid re-placing immediately
                    await self.event_queue.put(SystemAlertEvent("WARNING", f"TP {tp_level_num} failed for {symbol}: {order_status}"))


            elif purpose.startswith("STOP_LOSS") or purpose.startswith("TRAILING_STOP_UPDATE") or purpose.startswith("COMMAND_FORCE_CLOSE") or purpose.startswith("EMERGENCY_CLOSE_SHUTDOWN"):
                # Handle Stop Loss, Trailing Stop Loss, or forced close order updates
                if order_status == "filled":
                    LOGGER.critical(f"PRM: {purpose.replace('_', ' ')} for {position_id[:8]} ({symbol}) FILLED at {event.fill_price:.4f}. Amount: {event.filled_amount:.8f}.")
                    
                    # Final PnL for the closed position
                    pnl_on_close = calculate_pnl_usd(position["entry_price"], event.fill_price, event.filled_amount, position["side"])
                    position["realized_pnl"] += pnl_on_close # This will hold final PnL for full closure
                    position["total_fees"] += event.fee
                    position["current_amount"] -= event.filled_amount # Should become 0

                    # Update global PnL metrics
                    SHARED_STATE["global_capital"]["lifetime_realized_pnl"] += pnl_on_close
                    SHARED_STATE["global_capital"]["lifetime_fees_paid"] += event.fee
                    SHARED_STATE["global_capital"]["daily_pnl"] += pnl_on_close
                    SHARED_STATE["global_capital"]["daily_fees"] += event.fee

                    # Update strategy performance metrics
                    strat_perf = SHARED_STATE["strategy_performance_metrics"][position["strategy_id"]]
                    strat_perf["total_pnl_usd"] += pnl_on_close
                    strat_perf["daily_pnl_usd"] += pnl_on_close
                    if pnl_on_close < 0: # Increment consecutive losses on actual loss
                        strat_perf["consecutive_losses"] += 1
                        if strat_perf["consecutive_losses"] >= self.risk_config["max_consecutive_losses_per_strategy"]:
                            LOGGER.critical(f"PRM: Strategy {position['strategy_id']} hit max consecutive losses ({strat_perf['consecutive_losses']}). Activating kill switch: {self.risk_config['kill_switch_action']}")
                            if self.risk_config["kill_switch_action"] == "DISABLE_STRATEGY":
                                await self.event_queue.put(BotCommandEvent("PAUSE_STRATEGY", target=position["strategy_id"], details={"reason": "MaxConsecutiveLosses"}))
                            elif self.risk_config["kill_switch_action"] == "ACTIVATE_GLOBAL_KILL_SWITCH":
                                await self.event_queue.put(BotCommandEvent("ACTIVATE_GLOBAL_KILL_SWITCH", details={"reason": "MaxConsecutiveLossesGlobal"}))
                    else:
                        strat_perf["consecutive_losses"] = 0 # Reset on profit

                    # Finalize position (remove from active positions, move to history)
                    await self._close_position_finalize(position, event.fill_price, purpose)

                elif order_status == "canceled" and (purpose == "STOP_LOSS" or purpose == "TRAILING_STOP_UPDATE"):
                    LOGGER.warning(f"PRM: SL/TSL order {event.exchange_order_id} for {symbol} cancelled. This is expected if replaced by new TSL or manually cancelled.")
                    # Do not remove from active_positions or update PnL here, as position is still open.
                    # This event is for the ExecutionHandler to confirm cancellation.
                elif order_status == "rejected" and (purpose == "STOP_LOSS" or purpose == "TRAILING_STOP_UPDATE"):
                    LOGGER.error(f"PRM: SL/TSL order {event.exchange_order_id} for {symbol} REJECTED! This is critical. Manual intervention may be needed to manage risk. Details: {event.order_details.get('info')}")
                    await self.event_queue.put(SystemAlertEvent("CRITICAL", f"PRM: SL Rejected for {symbol}! Manual check needed!", details={"order_id": event.exchange_order_id, "symbol": symbol, "reason": event.order_details.get('info')}))

            # Update last_update_timestamp for the position
            if position:
                position["last_update_timestamp"] = get_current_utc_ms()
            LOGGER.debug(f"PRM: OrderUpdateEvent for {event.exchange_order_id} processed for position {position_id[:8]}.")

    async def _place_initial_exit_orders(self, position: Dict):
        """
        Places the initial Stop Loss and Take Profit orders for a newly opened position.
        This is called once the entry order is filled.
        [Refonte: Placement des ordres de sortie initiaux]
        """
        symbol = position["symbol"]
        amount = position["current_amount"] # Use actual filled amount
        
        # Determine the effective side for exit orders
        exit_side = "SELL" if position["side"] == "BUY" else "BUY"

        # Place Stop Loss Order (usually a STOP_MARKET order)
        if position["initial_stop_loss"]:
            sl_price_adjusted = adjust_price_for_order_conservative(symbol, position["initial_stop_loss"], exit_side, True, self.exchange_client)
            if sl_price_adjusted > 0:
                sl_order_request = OrderRequestEvent(
                    request_type="CREATE",
                    symbol=symbol,
                    order_type="STOP_MARKET", 
                    side=exit_side,
                    amount=amount, # Total amount
                    price=None, # Not applicable for stop market price
                    purpose="STOP_LOSS",
                    position_id=position["position_id"],
                    params={"stopPrice": sl_price_adjusted}, # Stop price for Binance futures
                    algo_params={"algo_type": "DEFAULT_MARKET"} # Use default market execution
                )
                await self.event_queue.put(sl_order_request)
                # Store the order ID locally (will be updated on OrderUpdateEvent)
                async with STATE_LOCK:
                    SHARED_STATE["active_positions"][position["position_id"]]["stop_loss_order_id"] = sl_order_request.client_order_id # Store client ID for reconciliation
                LOGGER.info(f"PRM: Placed initial SL order for {symbol} @ {sl_price_adjusted:.4f}. PosID: {position['position_id'][:8]}.")
            else:
                LOGGER.error(f"PRM: Invalid SL price calculated {sl_price_adjusted:.4f} for {symbol}. SL order not placed.")
        else:
            LOGGER.warning(f"PRM: No initial SL defined for {symbol}. Skipping SL order placement.")

        # Place Take Profit Orders (can be multiple LIMIT orders)
        tp_order_ids = []
        for i, tp_level in enumerate(position["take_profit_levels"]):
            if tp_level["status"] == "pending":
                # Amount for this specific TP level (percentage of original_amount)
                tp_amount_raw = position["original_amount"] * (tp_level["amount_pct_original"] / 100.0)
                tp_amount_adjusted = adjust_amount_for_order(symbol, tp_amount_raw, self.exchange_client)
                
                tp_price_adjusted = adjust_price_for_order_conservative(symbol, tp_level["price"], exit_side, False, self.exchange_client)

                # Ensure amount is tradable (not dust) and price is valid
                if tp_amount_adjusted > 1e-9 and tp_price_adjusted > 0:
                    tp_order_request = OrderRequestEvent(
                        request_type="CREATE",
                        symbol=symbol,
                        order_type="LIMIT",
                        side=exit_side,
                        amount=tp_amount_adjusted,
                        price=tp_price_adjusted,
                        purpose=f"TAKE_PROFIT_L{tp_level['level']}",
                        position_id=position["position_id"],
                        params={"timeInForce": self.trading_config["exit_order_time_in_force"]},
                        algo_params={"algo_type": "PASSIVE_LIMIT"} # Use passive limit execution
                    )
                    await self.event_queue.put(tp_order_request)
                    tp_order_ids.append(tp_order_request.client_order_id)
                    LOGGER.info(f"PRM: Placed initial TP Level {tp_level['level']} order for {symbol} @ {tp_price_adjusted:.4f} (Amount: {tp_amount_adjusted:.8f}).")
                else:
                    LOGGER.warning(f"PRM: Invalid TP amount ({tp_amount_adjusted:.8f}) or price ({tp_price_adjusted:.4f}) for {symbol} TP Level {tp_level['level']}. Order not placed.")
        
        async with STATE_LOCK:
            SHARED_STATE["active_positions"][position["position_id"]]["take_profit_order_ids"] = tp_order_ids


    async def _calculate_stop_loss(self, symbol: str, side: str, trigger_price: float, ohlcv_df: pd.DataFrame) -> Optional[float]:
        """
        Calculates the initial stop loss price based on ATR.
        Requires sufficient historical OHLCV data.
        """
        if ohlcv_df.empty or len(ohlcv_df) < self.risk_config["stop_loss_atr_period"] + 2:
            LOGGER.warning(f"PRM: Insufficient data for SL ATR calculation for {symbol}. Need at least {self.risk_config['stop_loss_atr_period'] + 2} candles.")
            return None
        
        high = ohlcv_df['high'].values
        low = ohlcv_df['low'].values
        close = ohlcv_df['close'].values

        atr = talib.ATR(high, low, close, timeperiod=self.risk_config["stop_loss_atr_period"])
        current_atr = atr[-1] if not np.isnan(atr[-1]) else 0.0

        if current_atr <= 1e-9: # If ATR is very small or zero
            LOGGER.warning(f"PRM: ATR value {current_atr:.8f} invalid/too small for {symbol} SL calculation. Cannot define meaningful stop.")
            return None

        sl_distance = current_atr * self.risk_config["stop_loss_atr_multiplier"]
        
        if side == "BUY":
            sl_price = trigger_price - sl_distance
        else: # SELL
            sl_price = trigger_price + sl_distance
        
        # Ensure SL price is positive and adjusted to exchange precision, conservatively
        sl_price = adjust_price_for_order_conservative(symbol, sl_price, side, is_stop_loss=True, exchange_instance=self.exchange_client)
        
        if sl_price <= 0:
            LOGGER.error(f"PRM: Calculated SL price is non-positive ({sl_price:.4f}) for {symbol}. Check parameters or market conditions.")
            return None
        return sl_price

    async def _calculate_take_profit(self, symbol: str, side: str, trigger_price: float, initial_sl_price: float) -> List[Dict]:
        """
        Calculates dynamic take profit levels based on Risk:Reward (R:R) ratios.
        Multiple TP levels can be defined in config.
        """
        tp_levels_info = []
        risk_per_unit = abs(trigger_price - initial_sl_price)
        
        if risk_per_unit <= CONFIG["risk_management"]["min_price_tick_distance"]:
            LOGGER.warning(f"PRM: Risk per unit too small ({risk_per_unit:.8f}) for TP calculation for {symbol}. Skipping TP levels.")
            return []

        if not self.risk_config["take_profit_levels"]:
            # Fallback to a single TP based on default R:R if no specific levels configured
            tp_rr = self.risk_config["take_profit_rr_ratio"]
            tp_distance = risk_per_unit * tp_rr
            tp_price = trigger_price + tp_distance if side == "BUY" else trigger_price - tp_distance
            tp_price = adjust_price_for_order_conservative(symbol, tp_price, side, is_stop_loss=False, exchange_instance=self.exchange_client)
            if tp_price > 0:
                tp_levels_info.append({"level": 1, "price": tp_price, "amount_pct_original": 100, "status": "pending"})
        else:
            total_pct_sum = 0
            for i, level_conf in enumerate(self.risk_config["take_profit_levels"]):
                tp_rr = level_conf["rr"]
                tp_pct = level_conf["pct"] # Percentage of original position to close at this level
                
                tp_distance = risk_per_unit * tp_rr
                tp_price = trigger_price + tp_distance if side == "BUY" else trigger_price - tp_distance
                
                tp_price_adjusted = adjust_price_for_order_conservative(symbol, tp_price, side, is_stop_loss=False, exchange_instance=self.exchange_client)
                if tp_price_adjusted <= 0: continue # Skip invalid prices
                
                total_pct_sum += tp_pct
                tp_levels_info.append({
                    "level": i + 1, "price": tp_price_adjusted, "amount_pct_original": tp_pct, "status": "pending"
                })
            
            # Normalize percentages if their sum is not 100 (e.g., [30, 30, 30] -> [33.3, 33.3, 33.3])
            if total_pct_sum != 100 and tp_levels_info:
                LOGGER.warning(f"PRM: Configured TP percentages sum to {total_pct_sum}%. Normalizing to 100%.")
                for level in tp_levels_info:
                    level["amount_pct_original"] = level["amount_pct_original"] * (100 / total_pct_sum)

        return tp_levels_info

    async def _manage_open_positions(self):
        """
        Iterates through active open positions, updates their unrealized PnL,
        and applies Stop Loss, Trailing Stop Loss, and Take Profit logic.
        [Refonte: Gestion proactive des positions]
        """
        async with STATE_LOCK:
            # Create a shallow copy to iterate over, preventing issues if `active_positions` is modified concurrently
            positions_to_manage = list(SHARED_STATE["active_positions"].items())

        if not positions_to_manage:
            LOGGER.debug("PRM: No open positions to manage in this cycle.")
            return

        current_prices = {}
        # Pre-fetch current prices for all relevant symbols to reduce lock contention
        for symbol in {p_data["symbol"] for _, p_data in positions_to_manage if p_data["status"] == "open"}:
            current_prices[symbol] = self._get_current_price_from_data_cache(symbol)
            if not current_prices[symbol]:
                LOGGER.warning(f"PRM: Current price for {symbol} not available in cache. Skipping position management for this symbol.")
                continue

        for pos_id, position in positions_to_manage:
            # Ensure the position is still actively managed and open
            # Re-check from SHARED_STATE for the very latest state in case it was closed/updated by another event
            async with STATE_LOCK:
                latest_position_state = SHARED_STATE["active_positions"].get(pos_id)
            
            if not latest_position_state or latest_position_state["status"] != "open": 
                continue # Skip if position is no longer open or managed

            symbol = latest_position_state["symbol"]
            current_price = current_prices.get(symbol)
            if current_price is None or current_price <= 0:
                LOGGER.warning(f"PRM: Cannot manage position {pos_id[:8]} for {symbol}: Current price unavailable. Skipping.")
                continue

            async with STATE_LOCK: # Acquire lock to update shared state
                # Update unrealized PnL for display and drawdown calculations
                entry_px = latest_position_state['entry_price']
                curr_amt = latest_position_state['current_amount']
                pos_side = latest_position_state['side']
                
                latest_position_state['pnl_usd'] = calculate_pnl_usd(entry_px, current_price, curr_amt, pos_side)
                if latest_position_state.get('margin_used', 0) > 1e-9:
                    latest_position_state['pnl_percentage'] = (latest_position_state['pnl_usd'] / latest_position_state['margin_used']) * 100
                else: latest_position_state['pnl_percentage'] = 0.0

                # Update highest/lowest profit price (for Trailing Stop Loss activation/movement)
                if pos_side == 'BUY':
                    latest_position_state['highest_profit_price'] = max(latest_position_state.get('highest_profit_price', entry_px), current_price)
                else: # SELL
                    latest_position_state['lowest_profit_price'] = min(latest_position_state.get('lowest_profit_price', entry_px), current_price)
                latest_position_state['last_update_timestamp'] = get_current_utc_ms()

                # Update daily max drawdown for this specific position/symbol
                current_daily_pnl_pct = (latest_position_state['pnl_usd'] / (entry_px * latest_position_state['original_amount'])) * 100 if entry_px * latest_position_state['original_amount'] > 1e-9 else 0
                SHARED_STATE["symbol_daily_drawdown"][symbol] = min(SHARED_STATE["symbol_daily_drawdown"][symbol], current_daily_pnl_pct)

                # Kill switch based on daily symbol drawdown
                if abs(SHARED_STATE["symbol_daily_drawdown"][symbol]) >= self.risk_config["max_daily_drawdown_per_symbol_pct"]:
                    if SHARED_STATE["live_strategy_parameters"].get(latest_position_state["strategy_id"], {}).get("enabled", True):
                        LOGGER.critical(f"PRM: Daily drawdown for {symbol} ({SHARED_STATE['symbol_daily_drawdown'][symbol]:.2f}%) exceeded {self.risk_config['max_daily_drawdown_per_symbol_pct']}%. Disabling strategy {latest_position_state['strategy_id']}.")
                        await self.event_queue.put(BotCommandEvent("PAUSE_STRATEGY", target=latest_position_state["strategy_id"], details={"reason": f"DailyDrawdownExceeded_{symbol}"}))
                        # Consider closing the position immediately
                        # await self.event_queue.put(OrderRequestEvent(request_type="CREATE", symbol=symbol, order_type="MARKET", side=exit_side, amount=latest_position_state["current_amount"], purpose="DAILY_DRAWDOWN_KILL"))
            
            # Fetch latest OHLCV data for TSL calculation (outside the lock if possible, but often tied to shared state)
            async with STATE_LOCK:
                ohlcv_df_1m = SHARED_STATE["ohlcv_data"][symbol].get("1m")
            
            # 1. Check Trailing Stop Loss & Move SL to BE logic
            await self._check_and_update_trailing_stop_loss(latest_position_state, current_price, ohlcv_df_1m) # [Refonte: TSL avancé]
            
            # 2. Check Hard Stop Loss (the absolute stop, can be moved by TSL/BE)
            await self._check_hard_stop_loss(latest_position_state, current_price)

            # 3. Check Take Profit levels
            await self._check_take_profit_levels(latest_position_state, current_price)
            
            async with STATE_LOCK:
                SHARED_STATE["telemetry_data"]["positions_managed"] = SHARED_STATE["telemetry_data"].get("positions_managed", 0) + 1

    def _get_current_price_from_data_cache(self, symbol: str) -> Optional[float]:
        """
        Retrieves the most recent price from the local data cache (1s OHLCV or Orderbook).
        Prioritizes 1s OHLCV close, then order book mid-price.
        """
        with STATE_LOCK:
            ohlcv_1s_df = SHARED_STATE["ohlcv_data"][symbol].get("1s")
            if ohlcv_1s_df is not None and not ohlcv_1s_df.empty:
                return ohlcv_1s_df['close'].iloc[-1]
            
            order_book = SHARED_STATE["order_book_snapshots"][symbol]
            if order_book.get("bids") and order_book.get("asks"):
                best_bid_price = max(order_book["bids"].keys()) if order_book["bids"] else 0.0
                best_ask_price = min(order_book["asks"].keys()) if order_book["asks"] else float('inf')
                if best_bid_price > 0 and best_ask_price != float('inf'):
                    return (best_bid_price + best_ask_price) / 2
        return None

    async def _check_hard_stop_loss(self, position: Dict, current_price: float):
        """
        Checks if the hard stop loss (current_stop_loss) for a position has been hit.
        If hit, sends a MARKET order to close the position.
        """
        sl_price = position["current_stop_loss"]
        if sl_price is None or sl_price <= 0: return # No valid SL set

        sl_hit = False
        if position["side"] == "BUY" and current_price <= sl_price:
            sl_hit = True
        elif position["side"] == "SELL" and current_price >= sl_price:
            sl_hit = True

        if sl_hit:
            LOGGER.critical(f"PRM: HARD STOP LOSS HIT for {position['symbol']} (PosID: {position['position_id'][:8]}). Current Price: {current_price:.4f}, SL: {sl_price:.4f}.")
            # Send MARKET order to close the position immediately
            await self.event_queue.put(OrderRequestEvent(
                request_type="CREATE",
                symbol=position["symbol"],
                order_type=self.trading_config["default_exit_order_type"], # Usually MARKET for immediate SL
                side="SELL" if position["side"] == "BUY" else "BUY",
                amount=position["current_amount"],
                price=None, # Market order, price not specified
                purpose="STOP_LOSS",
                position_id=position["position_id"],
                algo_params={"algo_type": "DEFAULT_MARKET", "urgency": "HIGH"} # Ensure fast execution
            ))
            # Mark position as "closing" to prevent re-triggering and further TSL/TP management
            async with STATE_LOCK:
                if position["position_id"] in SHARED_STATE["active_positions"]: # Double check existence
                    SHARED_STATE["active_positions"][position["position_id"]]["status"] = "closing"
                    SHARED_STATE["active_positions"][position["position_id"]]["last_update_timestamp"] = get_current_utc_ms()
            await self.event_queue.put(PositionUpdateEvent(position["position_id"], position["symbol"], "SL_HIT", {"price": current_price, "sl_price": sl_price}))

    async def _check_and_update_trailing_stop_loss(self, position: Dict, current_price: float, ohlcv_1m_df: pd.DataFrame):
        """
        Calculates and updates the trailing stop loss (TSL) for a position.
        Integrates moving SL to Break-Even after TP1.
        [Refonte: TSL Avancé, SL au BE]
        """
        if not self.risk_config["trailing_stop_atr_multiplier"] > 0: return # TSL disabled

        if ohlcv_1m_df.empty or len(ohlcv_1m_df) < self.risk_config["trailing_stop_atr_period"] + 2:
            LOGGER.debug(f"PRM: Insufficient 1m OHLCV data for TSL calculation for {position['symbol']}.")
            return

        # 1. Check Trailing Stop Loss Activation (R:R ratio)
        initial_sl_price = position["initial_stop_loss"]
        entry_price = position["entry_price"]
        
        # Calculate risk per unit for R:R
        risk_per_unit = abs(entry_price - initial_sl_price)
        if risk_per_unit <= CONFIG["risk_management"]["min_price_tick_distance"]: 
            LOGGER.debug(f"PRM: Risk per unit too small for TSL activation for {position['symbol']}. Skipping TSL.")
            return

        current_profit_per_unit = calculate_pnl_usd(entry_price, current_price, 1, position["side"])
        if current_profit_per_unit <= 0: return # No profit, TSL not applicable yet

        current_rr = current_profit_per_unit / risk_per_unit
        if current_rr < self.risk_config["trailing_stop_activation_rr"]:
            LOGGER.debug(f"PRM: TSL not activated for {position['symbol']}. Current R:R {current_rr:.2f} < Activation R:R {self.risk_config['trailing_stop_activation_rr']}.")
            return

        # 2. TSL is activated, calculate new potential TSL level based on ATR
        high_vals = ohlcv_1m_df['high'].values
        low_vals = ohlcv_1m_df['low'].values
        close_vals = ohlcv_1m_df['close'].values

        atr = talib.ATR(high_vals, low_vals, close_vals, timeperiod=self.risk_config["trailing_stop_atr_period"])
        current_atr = atr[-1] if not np.isnan(atr[-1]) else 0.0

        if current_atr <= 1e-9:
            LOGGER.warning(f"PRM: TSL ATR value {current_atr:.8f} invalid for {position['symbol']} TSL calculation. Skipping TSL update.")
            return

        tsl_distance = current_atr * self.risk_config["trailing_stop_atr_multiplier"]
        
        new_potential_tsl = 0.0
        if position["side"] == "BUY":
            # TSL trails the highest price reached since entry
            new_potential_tsl = position["highest_profit_price"] - tsl_distance
        else: # SELL
            # TSL trails the lowest price reached since entry
            new_potential_tsl = position["lowest_profit_price"] + tsl_distance
        
        # Adjust new TSL price for exchange precision and conservativeness
        new_potential_tsl = adjust_price_for_order_conservative(position["symbol"], new_potential_tsl, position["side"], True, self.exchange_client)

        # 3. Apply "Move SL to Break-Even After TP1 Hit" Logic (Priority)
        if self.risk_config["move_sl_to_be_after_tp1"]:
            tp1_level = next((lvl for lvl in position["take_profit_levels"] if lvl["level"] == 1), None)
            if tp1_level and tp1_level["status"] == "hit": # If TP1 has been hit
                be_price = position["entry_price"]
                # Add a small buffer to BE price to cover commissions/slippage
                if position["side"] == "BUY":
                    be_price = be_price * (1 + self.risk_config["be_buffer_pct"]/100) 
                else: # SELL
                    be_price = be_price * (1 - self.risk_config["be_buffer_pct"]/100)
                
                be_price_adjusted = adjust_price_for_order_conservative(position["symbol"], be_price, position["side"], True, self.exchange_client)
                
                # If BE price is "better" (protects more profit or reduces loss more) than current TSL or initial SL
                is_be_better = False
                if position["side"] == "BUY" and be_price_adjusted > position["current_stop_loss"]: is_be_better = True
                elif position["side"] == "SELL" and be_price_adjusted < position["current_stop_loss"]: is_be_better = True
                
                if is_be_better:
                    new_potential_tsl = be_price_adjusted # Overwrite TSL with BE if BE is superior
                    LOGGER.info(f"PRM: SL for {position['symbol']} moved to Break-Even ({new_potential_tsl:.4f}) after TP1 hit.")


        # 4. Compare new potential TSL with current_stop_loss and update only if it "improves" the stop
        current_stop = position["current_stop_loss"]
        should_move_sl = False

        if position["side"] == "BUY":
            # For BUY, new TSL should be higher than current SL (to lock in more profit or reduce loss)
            # Also, new TSL must be below current market price (with a small buffer) to avoid immediate hit
            if new_potential_tsl > current_stop and \
               new_potential_tsl < current_price - (CONFIG["risk_management"]["min_price_tick_distance"] * 2): 
                should_move_sl = True
        else: # SELL
            # For SELL, new TSL should be lower than current SL
            # Also, new TSL must be above current market price (with a small buffer)
            if new_potential_tsl < current_stop and \
               new_potential_tsl > current_price + (CONFIG["risk_management"]["min_price_tick_distance"] * 2): 
                should_move_sl = True
        
        if should_move_sl:
            LOGGER.info(f"PRM: Moving SL for {position['symbol']} (PosID: {position['position_id'][:8]}) from {current_stop:.4f} to {new_potential_tsl:.4f} (Reason: TSL/BE).")
            # Define exit_side as the opposite of position side
            exit_side = "SELL" if position["side"] == "BUY" else "BUY"
            # Request to modify the existing SL order on the exchange
            await self.event_queue.put(OrderRequestEvent(
                request_type="MODIFY",
                symbol=position["symbol"],
                order_type="STOP_MARKET", # Assuming SL is always stop market for futures
                side=exit_side,
                amount=position["current_amount"], # Modify for current remaining amount
                price=None, # For stop market
                purpose="TRAILING_STOP_UPDATE" if current_rr >= self.risk_config["trailing_stop_activation_rr"] else "MOVE_SL_TO_BE",
                position_id=position["position_id"],
                original_order_id=position["stop_loss_order_id"], # The ID of the current SL order to modify
                params={"stopPrice": new_potential_tsl},
                algo_params={"algo_type": "DEFAULT_MARKET"} # Standard execution for stop modification
            ))
            # Optimistically update the internal position state
            async with STATE_LOCK:
                if position["position_id"] in SHARED_STATE["active_positions"]:
                    SHARED_STATE["active_positions"][position["position_id"]]["current_stop_loss"] = new_potential_tsl
                    SHARED_STATE["active_positions"][position["position_id"]]["current_trailing_stop_level"] = new_potential_tsl
                    SHARED_STATE["active_positions"][position["position_id"]]["last_update_timestamp"] = get_current_utc_ms()
            await self.event_queue.put(PositionUpdateEvent(position["position_id"], position["symbol"], "SL_MOVED", {"new_sl_price": new_potential_tsl}))
    async def _check_take_profit_levels(self, position: Dict, current_price: float):
        """
        Checks if any take profit level for a position has been hit.
        If hit, sends a LIMIT order to close the corresponding portion of the position.
        """
        if not position["take_profit_levels"]: return # No TP levels defined

        exit_side = "SELL" if position["side"] == "BUY" else "BUY"

        for i, tp_level in enumerate(position["take_profit_levels"]):
            if tp_level["status"] == "pending": # Only consider pending TP levels
                tp_price = tp_level["price"]
                
                tp_hit = False
                # Check if current price crossed the TP level
                if position["side"] == "BUY" and current_price >= tp_price:
                    tp_hit = True
                elif position["side"] == "SELL" and current_price <= tp_price:
                    tp_hit = True
                
                if tp_hit:
                    LOGGER.info(f"PRM: Take Profit Level {tp_level['level']} HIT for {position['symbol']} (PosID: {position['position_id'][:8]}). Current Price: {current_price:.4f}, Target: {tp_price:.4f}.")
                    
                    # Calculate amount to close for this TP level. It's a percentage of ORIGINAL amount.
                    # This ensures TP levels close the intended proportion even with partial fills on entry.
                    amount_to_close_raw = position["original_amount"] * (tp_level["amount_pct_original"] / 100.0)
                    amount_to_close_adjusted = adjust_amount_for_order(position["symbol"], amount_to_close_raw, self.exchange_client)
                    
                    # Ensure the amount is tradeable and not just "dust"
                    if amount_to_close_adjusted <= CONFIG["exchange"]["min_notional_multiplier"] * 0.0001: 
                        LOGGER.warning(f"PRM: TP Level {tp_level['level']} for {position['symbol']} hit, but calculated amount to close ({amount_to_close_adjusted:.8f}) is too small/dust. Marking as hit but no order placed.")
                        async with STATE_LOCK:
                            position["take_profit_levels"][i]["status"] = "hit"
                            position["last_update_timestamp"] = get_current_utc_ms()
                        continue

                    # Request to create a LIMIT order for this TP level
                    tp_order_request = OrderRequestEvent(
                        request_type="CREATE",
                        symbol=position["symbol"],
                        order_type="LIMIT",
                        side=exit_side,
                        amount=amount_to_close_adjusted,
                        price=tp_price, # Limit price is the TP target
                        purpose=f"TAKE_PROFIT_L{tp_level['level']}",
                        position_id=position["position_id"],
                        params={"timeInForce": self.trading_config["exit_order_time_in_force"]},
                        algo_params={"algo_type": "TWAP" if amount_to_close_adjusted * tp_price > 50000 else "PASSIVE_LIMIT"} # Use TWAP for larger TP amounts
                    )
                    await self.event_queue.put(tp_order_request)

                    # Mark TP level as "pending_order" to avoid re-triggering (status will update on OrderUpdateEvent)
                    async with STATE_LOCK:
                        position["take_profit_levels"][i]["status"] = "pending_order"
                        # Add clientOrderId to track this specific TP order
                        position["take_profit_order_ids"].append(tp_order_request.client_order_id) 
                        position["last_update_timestamp"] = get_current_utc_ms()

                    await self.event_queue.put(PositionUpdateEvent(position["position_id"], position["symbol"], "TP_HIT", {"level": tp_level['level'], "price": current_price, "amount": amount_to_close_adjusted}))
    async def _move_sl_to_break_even_logic(self, position: Dict):
        """
        Internal logic to move the stop loss to break-even (or a small profit buffer)
        after the first take profit level has been hit.
        """
        # Calculate the break-even price including a small buffer for fees/slippage
        be_price = position["entry_price"]
        if position["side"] == "BUY":
            be_price = be_price * (1 + self.risk_config["be_buffer_pct"]/100) 
        else: # SELL
            be_price = be_price * (1 - self.risk_config["be_buffer_pct"]/100)
        
        be_price_adjusted = adjust_price_for_order_conservative(position["symbol"], be_price, "SELL" if position["side"] == "BUY" else "BUY", True, self.exchange_client)

        current_sl = position["current_stop_loss"]
        should_move_to_be = False

        if position["side"] == "BUY" and be_price_adjusted > current_sl:
            should_move_to_be = True
        elif position["side"] == "SELL" and be_price_adjusted < current_sl:
            should_move_to_be = True

        if should_move_to_be:
            LOGGER.info(f"PRM: Requesting SL move to Break-Even ({be_price_adjusted:.4f}) for {position['symbol']} (PosID: {position['position_id'][:8]}) after TP1 hit.")
            # Send an OrderRequestEvent to ExecutionHandler to modify the SL order
            await self.event_queue.put(OrderRequestEvent(
                request_type="MODIFY",
                symbol=position["symbol"],
                order_type="STOP_MARKET",
                side="SELL" if position["side"] == "BUY" else "BUY",
                amount=position["current_amount"],
                price=None,
                purpose="MOVE_SL_TO_BE",
                position_id=position["position_id"],
                original_order_id=position["stop_loss_order_id"], # The ID of the existing SL order
                params={"stopPrice": be_price_adjusted},
                algo_params={"algo_type": "DEFAULT_MARKET"}
            ))
            # Optimistically update internal state (will be confirmed by EH's OrderUpdateEvent)
            async with STATE_LOCK:
                SHARED_STATE["active_positions"][position["position_id"]]["current_stop_loss"] = be_price_adjusted
                SHARED_STATE["active_positions"][position["position_id"]]["last_update_timestamp"] = get_current_utc_ms()
            await self.event_queue.put(PositionUpdateEvent(position["position_id"], position["symbol"], "SL_MOVED_TO_BE", {"new_sl_price": be_price_adjusted}))


    async def _close_position_finalize(self, position: Dict, final_fill_price: float, reason: str):
        """
        Finalizes a position after it has been fully closed (either by SL, TP, or force close).
        Removes it from active positions and adds it to the trade history.
        """
        async with STATE_LOCK:
            # Remove from active positions
            if position["position_id"] in SHARED_STATE["active_positions"]:
                SHARED_STATE["active_positions"].pop(position["position_id"])
            
            # Update final status and timestamps
            position["status"] = "closed"
            position["close_time"] = get_current_utc_ms() / 1000 # Convert ms to seconds
            position["close_price"] = final_fill_price
            
            # Calculate duration (ensure entry_time is also in seconds)
            entry_time_sec = position["entry_time"] if isinstance(position["entry_time"], (float, int)) else position["entry_time"].timestamp()
            position["duration_seconds"] = position["close_time"] - entry_time_sec

            # Add to trade history
            SHARED_STATE["trade_history"].append(copy.deepcopy(position))
            
            LOGGER.info(f"PRM: POSITION FINALIZED: {position['symbol']} {position['side']} (PosID: {position['position_id'][:8]}). Reason: {reason}. Realized PnL: {position['realized_pnl']:.4f} USD. Total Fees: {position['total_fees']:.4f} USD.")
            await self.event_queue.put(PositionUpdateEvent(position["position_id"], position["symbol"], "CLOSED", {
                "reason": reason, "close_price": final_fill_price, "realized_pnl_usd": position['realized_pnl'],
                "total_fees_paid": position['total_fees'], "duration_seconds": position['duration_seconds']
            }))
            # Request ExecutionHandler to cancel any remaining open orders for this position (SL/TP orders)
            await self.event_queue.put(OrderRequestEvent(
                request_type="CANCEL_ALL_FOR_POS",
                symbol=position["symbol"],
                position_id=position["position_id"],
                purpose="CLEANUP_POSITION_CLOSED"
            ))


    async def stop(self):
        self._running = False
        LOGGER.info("PortfolioRiskManager stop requested. Closing exchange client.")
        if self.exchange_client:
            await self.exchange_client.close()


# --- NEW: Base Execution Algorithm Class ---
class ExecutionAlgo:
    """
    Base class for execution algorithms. Defines the interface for different algo types.
    [Refonte: Suite d'algorithmes d'exécution]
    """
    def __init__(self, exchange_client: Any, event_queue: asyncio.Queue, config: Dict):
        self.exchange_client = exchange_client
        self.event_queue = event_queue
        self.config = config
        self.execution_config = config["execution"]
        self.trading_config = config["trading"]
        self.logger = LOGGER
        self.active_orders: Dict[str, Dict] = {} # Orders currently being managed by this algo (e.g., TWAP slices)

    async def execute_order(self, order_request: OrderRequestEvent) -> Optional[Dict]:
        """
        Executes the main logic for placing an order using this algorithm.
        Returns the final CCXT order object if successful, or None.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("execute_order method must be implemented by subclass.")

    async def cancel_order(self, order_id: str, symbol: str, purpose: str):
        """Cancels an order managed by this algorithm."""
        raise NotImplementedError("cancel_order method must be implemented by subclass.")

    async def monitor_algo_orders(self):
        """
        (Optional) Monitors ongoing orders for the algorithm (e.g., checking TWAP slice fills).
        This would be a background task managed by the ExecutionHandler.
        """
        pass

    @retry_async(max_retries=CONFIG["execution"]["order_retry_attempts"], delay_seconds=CONFIG["execution"]["order_retry_delay_seconds"], catch_exceptions=(ccxt.NetworkError, ccxt.ExchangeError))
    @CircuitBreaker(failure_threshold=CONFIG["exchange"]["api_rate_limits_per_second"]["rest_private"] * 5, recovery_timeout=60, logger=LOGGER)
    async def _place_single_order(self, symbol: str, order_type: str, side: str, amount: float, price: Optional[float], params: Dict) -> Optional[Dict]:
        """
        Internal helper to place a single order on the exchange with retries and circuit breaker.
        Used by all execution algorithms.
        """
        adjusted_amount = adjust_amount_for_order(symbol, amount, self.exchange_client)
        adjusted_price = adjust_price_for_order(symbol, price, self.exchange_client) if price else None
        
        if adjusted_amount <= 1e-9:
            self.logger.warning(f"ExecutionAlgo: Order {order_type} {side} for {symbol} with amount {amount:.8f} adjusted to zero. Order not placed.")
            return None
        
        # CCXT order type mapping (e.g., STOP_MARKET handled by params)
        ccxt_order_type = order_type.lower()
        ccxt_price_param = adjusted_price
        if 'stopPrice' in params:
            if ccxt_order_type == 'stop_market': ccxt_order_type = 'market' # Binance uses market type with stopPrice param
            if ccxt_order_type == 'stop_limit': ccxt_price_param = adjusted_price # For stop-limit, price param is the limit price
            if ccxt_order_type == 'market' and ccxt_price_param is not None: # Market order shouldn't have price param
                ccxt_price_param = None
        
        try:
            order = await self.exchange_client.create_order(
                symbol, ccxt_order_type, side, adjusted_amount, ccxt_price_param, params
            )
            
            async with STATE_LOCK:
                SHARED_STATE["open_orders"][order["id"]] = order # Track all open orders centrally
                SHARED_STATE["client_order_map"][order.get("clientOrderId")] = order["id"]
                SHARED_STATE["telemetry_data"]["orders_placed"] = SHARED_STATE["telemetry_data"].get("orders_placed", 0) + 1
            
            self.logger.info(f"ExecutionAlgo: Order {order['id']} ({ccxt_order_type} {side} {symbol}) submitted. Status: {order['status']}.")
            return order
        
        except ccxt.ArgumentsRequired as e:
            self.logger.error(f"ExecutionAlgo: Missing arguments for order {symbol}: {e}. Check parameters.", exc_info=True)
            return None
        except ccxt.InvalidOrder as e:
            self.logger.error(f"ExecutionAlgo: Invalid order for {symbol}: {e}. Check amount, price, type, or exchange limits.", exc_info=True)
            return None
        except ccxt.InsufficientFunds as e:
            self.logger.error(f"ExecutionAlgo: Insufficient funds for {symbol}: {e}. Order not placed.", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("ERROR", f"EXH: Insufficient Funds for {symbol}. Order not placed."))
            return None
        except Exception as e:
            self.logger.error(f"ExecutionAlgo: Unexpected error creating order for {symbol}: {e}. Retrying if possible.", exc_info=True)
            raise # Re-raise for retry decorator

    async def _cancel_single_order(self, order_id: str, symbol: str, purpose: str) -> bool:
        """Internal helper to cancel a single order with retries and circuit breaker."""
        try:
            cancelled_order = await self.exchange_client.cancel_order(order_id, symbol)
            self.logger.info(f"ExecutionAlgo: Order {order_id} ({symbol}) cancelled successfully. Purpose: {purpose}.")
            # Update internal state (EH will handle global SHARED_STATE["open_orders"] update via OrderUpdateEvent)
            self.active_orders.pop(order_id, None)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning(f"ExecutionAlgo: Attempted to cancel order {order_id} ({symbol}), but it was not found on exchange. Already closed/canceled?")
            self.active_orders.pop(order_id, None) # Remove locally
            return True # Consider it successfully "removed" from exchange context
        except CircuitBreakerOpenError:
            self.logger.warning(f"ExecutionAlgo: Circuit Breaker OPEN for cancel. Skipping cancellation of {order_id}.")
            return False
        except Exception as e:
            self.logger.error(f"ExecutionAlgo: Failed to cancel order {order_id} ({symbol}): {e}.", exc_info=True)
            return False


# --- Concrete Execution Algorithms ---

class DefaultMarketAlgo(ExecutionAlgo):
    """
    Executes a market order. Simple and immediate, high urgency.
    """
    def __init__(self, exchange_client: Any, event_queue: asyncio.Queue, config: Dict):
        super().__init__(exchange_client, event_queue, config)
        self.purpose_mapping = {
            "ENTRY_": "MARKET_ENTRY", 
            "STOP_LOSS": "MARKET_STOP_LOSS", 
            "COMMAND_FORCE_CLOSE": "MARKET_FORCE_CLOSE",
            "EMERGENCY_CLOSE_SHUTDOWN": "MARKET_EMERGENCY_CLOSE"
        }

    async def execute_order(self, order_request: OrderRequestEvent) -> Optional[Dict]:
        symbol = order_request.symbol
        side = order_request.side
        amount = order_request.amount
        purpose = order_request.purpose
        pos_id = order_request.position_id
        
        # Ensure 'reduceOnly' is set for futures exit orders if configured
        final_params = order_request.params.copy()
        if self.trading_config["use_reduce_only"] and self.exchange_client.options.get('defaultType') == 'future' and purpose.startswith(("STOP_LOSS", "TAKE_PROFIT", "COMMAND_FORCE_CLOSE", "EMERGENCY_CLOSE")):
            final_params["reduceOnly"] = True
        final_params["clientOrderId"] = order_request.client_order_id

        # For Stop Market orders, price param is 'stopPrice' in params
        order_type = order_request.order_type # This could be "MARKET" or "STOP_MARKET"
        if order_type.upper() == "STOP_MARKET":
            # For Binance, a STOP_MARKET is a 'market' type order with 'stopPrice' in params
            final_params["stopPrice"] = order_request.params.get("stopPrice")
            ccxt_order_type = "market"
            price_for_ccxt = None
        else:
            ccxt_order_type = "market"
            price_for_ccxt = None # Market orders don't have a limit price

        LOGGER.info(f"DefaultMarketAlgo: Placing MARKET order for {symbol} {side} {amount:.8f} (Purpose: {purpose}).")
        order = await self._place_single_order(symbol, ccxt_order_type, side, amount, price_for_ccxt, final_params)
        
        if order:
            await self._emit_order_update_event(order, purpose, pos_id)
        return order

    async def cancel_order(self, order_id: str, symbol: str, purpose: str):
        # Market orders typically fill instantly, so cancellation is rarely needed.
        # If it's a STOP_MARKET (which is a pending order), then it can be cancelled.
        if order_id in self.active_orders: # Check if this algo is managing it
            await self._cancel_single_order(order_id, symbol, purpose)
        else:
            LOGGER.warning(f"DefaultMarketAlgo: Request to cancel order {order_id} ({symbol}) but it's not actively managed by this algo or already filled.")
            # For stop orders, they stay active. It's crucial to cancel the specific STOP order ID.
            # We assume ExecutionHandler will look it up if this algo doesn't track it.
    
    async def _emit_order_update_event(self, order: Dict, purpose: str, pos_id: str):
        """Helper to emit OrderUpdateEvent, centralizing this action."""
        status = order.get('status', 'unknown').lower()
        filled_amount = to_float(order.get('filled', 0.0))
        remaining_amount = to_float(order.get('remaining', order.get('amount', 0.0)) - filled_amount)
        fill_price = to_float(order.get('average', order.get('price', 0.0)))
        fee_cost = to_float(order.get('fee', {}).get('cost', 0.0))
        fee_currency = order.get('fee', {}).get('currency', self.trading_config["capital_currency"])
        
        update_event = OrderUpdateEvent(
            exchange_order_id=order["id"],
            client_order_id=order.get("clientOrderId", "N/A"),
            symbol=order["symbol"],
            status=status,
            filled_amount=filled_amount,
            remaining_amount=remaining_amount,
            fill_price=fill_price,
            purpose=purpose,
            position_id=pos_id,
            fee=fee_cost,
            fee_currency=fee_currency,
            order_details=order 
        )
        await self.event_queue.put(update_event)
        LOGGER.debug(f"DefaultMarketAlgo: Emitted OrderUpdateEvent for {order['id']} ({order['symbol']}) - Status: {status}, Filled: {filled_amount}.")
        if status in ['filled', 'canceled', 'rejected', 'expired']:
            self.active_orders.pop(order["id"], None)


class PassiveLimitAlgo(ExecutionAlgo):
    """
    Executes a limit order passively, aiming for best possible fill without immediate market impact.
    Does not aggressively reposition.
    """
    def __init__(self, exchange_client: Any, event_queue: asyncio.Queue, config: Dict):
        super().__init__(exchange_client, event_queue, config)

    async def execute_order(self, order_request: OrderRequestEvent) -> Optional[Dict]:
        symbol = order_request.symbol
        side = order_request.side
        amount = order_request.amount
        price = order_request.price
        purpose = order_request.purpose
        pos_id = order_request.position_id

        if price is None:
            self.logger.error(f"PassiveLimitAlgo: Price not provided for LIMIT order {symbol}. Cannot place.")
            return None

        final_params = order_request.params.copy()
        final_params["clientOrderId"] = order_request.client_order_id

        LOGGER.info(f"PassiveLimitAlgo: Placing LIMIT order for {symbol} {side} {amount:.8f} @ {price:.5f} (Purpose: {purpose}).")
        order = await self._place_single_order(symbol, "limit", side, amount, price, final_params)
        
        if order:
            self.active_orders[order["id"]] = order # Track this order
            await self._emit_order_update_event(order, purpose, pos_id)
        return order

    async def cancel_order(self, order_id: str, symbol: str, purpose: str):
        if order_id in self.active_orders:
            success = await self._cancel_single_order(order_id, symbol, purpose)
            if success:
                # If successfully cancelled, update its status to 'canceled' for the event
                cancelled_order_info = self.active_orders.get(order_id, {})
                cancelled_order_info['status'] = 'canceled'
                await self._emit_order_update_event(cancelled_order_info, purpose, cancelled_order_info.get('info',{}).get('position_id', 'N/A'))
        else:
            LOGGER.warning(f"PassiveLimitAlgo: Request to cancel order {order_id} ({symbol}) but it's not actively managed by this algo.")

    async def _emit_order_update_event(self, order: Dict, purpose: str, pos_id: str):
        status = order.get('status', 'unknown').lower()
        filled_amount = to_float(order.get('filled', 0.0))
        remaining_amount = to_float(order.get('remaining', order.get('amount', 0.0)) - filled_amount)
        fill_price = to_float(order.get('average', order.get('price', 0.0)))
        fee_cost = to_float(order.get('fee', {}).get('cost', 0.0))
        fee_currency = order.get('fee', {}).get('currency', self.trading_config["capital_currency"])
        
        update_event = OrderUpdateEvent(
            exchange_order_id=order["id"],
            client_order_id=order.get("clientOrderId", "N/A"),
            symbol=order["symbol"],
            status=status,
            filled_amount=filled_amount,
            remaining_amount=remaining_amount,
            fill_price=fill_price,
            purpose=purpose,
            position_id=pos_id,
            fee=fee_cost,
            fee_currency=fee_currency,
            order_details=order 
        )
        await self.event_queue.put(update_event)
        if status in ['filled', 'canceled', 'rejected', 'expired']:
            self.active_orders.pop(order["id"], None)


class TWAPAlgo(ExecutionAlgo):
    """
    Time-Weighted Average Price (TWAP) algorithm.
    Slices a large order into smaller, time-based chunks to minimize market impact.
    [Refonte: TWAP]
    """
    def __init__(self, exchange_client: Any, event_queue: asyncio.Queue, config: Dict):
        super().__init__(exchange_client, event_queue, config)
        self.twap_orders: Dict[str, Dict] = {} # {main_order_id: {slices: [], total_amount, filled_amount, ...}}
        self.default_num_slices = self.execution_config["vwap_slicer_num_slices"] # Renamed from vwap to general slicer
        self.default_interval_seconds = self.execution_config["vwap_slicer_interval_seconds"]

    async def execute_order(self, order_request: OrderRequestEvent) -> Optional[Dict]:
        main_client_order_id = order_request.client_order_id # Use main client ID for TWAP group
        symbol = order_request.symbol
        side = order_request.side
        total_amount = order_request.amount
        limit_price = order_request.price # TWAP can be limit or market
        purpose = order_request.purpose
        pos_id = order_request.position_id
        
        algo_params = order_request.algo_params
        num_slices = algo_params.get("num_slices", self.default_num_slices)
        interval = algo_params.get("interval_seconds", self.default_interval_seconds)
        order_type_for_slices = algo_params.get("order_type_for_slices", order_request.order_type) # Can be MARKET or LIMIT

        if total_amount <= 1e-9:
            self.logger.warning(f"TWAPAlgo: Total amount for TWAP is zero or too small for {symbol}. Order not placed.")
            return None

        slice_amount = total_amount / num_slices
        
        self.twap_orders[main_client_order_id] = {
            "symbol": symbol, "side": side, "total_amount": total_amount, 
            "filled_amount": 0.0, "limit_price": limit_price, "purpose": purpose, "pos_id": pos_id,
            "slices": [], "is_completed": False, "main_order_request": order_request
        }
        
        self.logger.info(f"TWAPAlgo: Initiating TWAP for {symbol} {side} {total_amount:.8f} in {num_slices} slices over {num_slices * interval}s.")
        
        # Start a background task for slicing and placing orders
        asyncio.create_task(
            self._twap_slicing_task(main_client_order_id, symbol, side, total_amount, limit_price, purpose, pos_id, num_slices, interval, order_type_for_slices, order_request.params),
            name=f"TWAP_Slicing_{main_client_order_id[:8]}"
        )
        # Return a dummy order object or the initial order request as placeholder
        return {"id": main_client_order_id, "symbol": symbol, "type": "TWAP", "status": "open", "amount": total_amount, "filled": 0.0, "remaining": total_amount}

    async def _twap_slicing_task(self, main_client_order_id: str, symbol: str, side: str, total_amount: float, limit_price: Optional[float], purpose: str, pos_id: str, num_slices: int, interval: int, order_type_for_slices: str, base_params: Dict):
        twap_info = self.twap_orders[main_client_order_id]
        
        for i in range(num_slices):
            if not self._running or twap_info["is_completed"]: break # Stop if shutting down or main order completed
            
            remaining_amount = total_amount - twap_info["filled_amount"]
            if remaining_amount <= 1e-9:
                twap_info["is_completed"] = True
                self.logger.info(f"TWAPAlgo: TWAP for {main_client_order_id[:8]} completed. All amount filled.")
                break

            current_slice_amount = min(total_amount / num_slices, remaining_amount)
            # Ensure slice is tradable (not dust)
            current_slice_amount = adjust_amount_for_order(symbol, current_slice_amount, self.exchange_client)
            if current_slice_amount <= 1e-9:
                self.logger.warning(f"TWAPAlgo: Slice amount for {symbol} is too small ({current_slice_amount:.8f}). Skipping remaining slices.")
                break

            slice_client_order_id = f"{main_client_order_id}_slice{i+1}"
            slice_params = base_params.copy()
            slice_params["clientOrderId"] = slice_client_order_id
            
            # Use 'reduceOnly' for exit orders if applicable
            if self.trading_config["use_reduce_only"] and self.exchange_client.options.get('defaultType') == 'future' and purpose.startswith(("TAKE_PROFIT", "STOP_LOSS", "COMMAND_FORCE_CLOSE", "EMERGENCY_CLOSE")):
                slice_params["reduceOnly"] = True

            self.logger.debug(f"TWAPAlgo: Placing slice {i+1}/{num_slices} for {symbol}: {current_slice_amount:.8f} @ {limit_price:.5f}")
            
            slice_order = await self._place_single_order(symbol, order_type_for_slices, side, current_slice_amount, limit_price, slice_params)
            
            if slice_order:
                twap_info["slices"].append({"order_id": slice_order["id"], "client_order_id": slice_client_order_id, "amount": current_slice_amount, "status": slice_order["status"]})
                self.active_orders[slice_order["id"]] = slice_order # Track individual slice order
                
                # If slice fills immediately (e.g., Market order or aggressive limit fill)
                if slice_order["status"] == "filled":
                    twap_info["filled_amount"] += to_float(slice_order.get("filled", 0.0))
                    # Propagate this as a partial update for the main order
                    await self._emit_order_update_event(slice_order, f"{purpose}_TWAP_SLICE_FILLED", pos_id)
                else:
                    # Monitor this slice in background until filled or cancelled
                    asyncio.create_task(self._monitor_slice_fill(slice_order["id"], symbol, purpose, pos_id, main_client_order_id), name=f"TWAP_SliceMonitor_{slice_order['id'][:8]}")

            else:
                self.logger.error(f"TWAPAlgo: Failed to place slice {i+1} for {symbol}. Stopping TWAP slicing for {main_client_order_id[:8]}.")
                break # Stop slicing if a slice fails

            if i < num_slices - 1: # Don't sleep after the last slice
                await asyncio.sleep(interval)
        
        # After all slices are submitted or failed, mark TWAP as completed internally
        twap_info["is_completed"] = True
        if twap_info["filled_amount"] >= total_amount * 0.999: # Consider almost full as full
            LOGGER.info(f"TWAPAlgo: All TWAP slices for {main_client_order_id[:8]} submitted. Total filled: {twap_info['filled_amount']:.8f}/{total_amount:.8f}.")
            # Emit a final "filled" event for the main order (if not already fully reported by slice events)
            final_order_obj = twap_info["main_order_request"].data # Use the original request as base
            final_order_obj["status"] = "filled"
            final_order_obj["filled"] = twap_info["filled_amount"]
            final_order_obj["remaining"] = twap_info["total_amount"] - twap_info["filled_amount"]
            # For simplicity, use the last slice's average price or initial limit price.
            final_order_obj["average"] = twap_info["limit_price"] or (twap_info["filled_amount"] / total_amount) * twap_info["limit_price"] # Simplified
            await self._emit_order_update_event(final_order_obj, f"{purpose}_TWAP_FINAL", pos_id)
        else:
            LOGGER.warning(f"TWAPAlgo: TWAP for {main_client_order_id[:8]} finished, but only partially filled: {twap_info['filled_amount']:.8f}/{total_amount:.8f}.")
            # Emit a partial fill event or final cancelled if too much remained
            final_order_obj = twap_info["main_order_request"].data
            final_order_obj["status"] = "partial_fill" if twap_info["filled_amount"] > 0 else "canceled"
            final_order_obj["filled"] = twap_info["filled_amount"]
            final_order_obj["remaining"] = twap_info["total_amount"] - twap_info["filled_amount"]
            self._emit_order_update_event(final_order_obj, f"{purpose}_TWAP_PARTIAL_OR_FAILED", pos_id)


    async def _monitor_slice_fill(self, slice_order_id: str, symbol: str, purpose: str, pos_id: str, main_client_order_id: str):
        """Monitors an individual slice order until it's filled or cancelled."""
        timeout = self.execution_config["order_fill_timeout_seconds"] # Use general order fill timeout
        start_time = time.time()

        while self._running and (time.time() - start_time) < timeout:
            async with STATE_LOCK:
                order_in_shared_state = SHARED_STATE["open_orders"].get(slice_order_id)

            if order_in_shared_state and order_in_shared_state['status'] in ['open', 'partial']:
                # Order still active, check again after a short delay
                await asyncio.sleep(self.execution_config["order_status_check_interval_seconds"])
            elif order_in_shared_state and order_in_shared_state['status'] == 'filled':
                # Slice filled, update parent TWAP order's filled amount
                twap_info = self.twap_orders.get(main_client_order_id)
                if twap_info:
                    current_filled_by_slice = to_float(order_in_shared_state.get('filled', 0.0))
                    twap_info["filled_amount"] += current_filled_by_slice
                    LOGGER.debug(f"TWAPAlgo: Slice {slice_order_id[:8]} for {symbol} filled. Total TWAP filled: {twap_info['filled_amount']:.8f}.")
                    # No need to emit update here, _emit_order_update_event already sent it for the slice itself.
                break
            else: # Order not found in SHARED_STATE or its status is final (cancelled/rejected)
                if order_in_shared_state:
                    LOGGER.warning(f"TWAPAlgo: Slice {slice_order_id[:8]} for {symbol} finished with status: {order_in_shared_state['status']}.")
                else:
                    LOGGER.warning(f"TWAPAlgo: Slice {slice_order_id[:8]} for {symbol} disappeared from tracking.")
                break
        
        if not self._running or (time.time() - start_time) >= timeout:
            LOGGER.warning(f"TWAPAlgo: Slice {slice_order_id[:8]} for {symbol} timed out after {timeout}s or bot stopping. Attempting to cancel.")
            await self._cancel_single_order(slice_order_id, symbol, f"TWAP_SLICE_TIMEOUT_CANCEL")


    async def cancel_order(self, main_order_id: str, symbol: str, purpose: str):
        """
        Cancels a TWAP master order and all its active slices.
        """
        twap_info = self.twap_orders.get(main_order_id)
        if not twap_info:
            LOGGER.warning(f"TWAPAlgo: Cannot cancel TWAP order {main_order_id} for {symbol}. Not found or already completed/cancelled.")
            return

        LOGGER.info(f"TWAPAlgo: Cancelling TWAP master order {main_order_id} and all {len(twap_info['slices'])} active slices for {symbol}.")
        
        twap_info["is_completed"] = True # Signal slicing task to stop
        cancel_tasks = []
        for slice_data in twap_info["slices"]:
            slice_order_id = slice_data["order_id"]
            if slice_order_id in self.active_orders: # Only cancel if still active by this algo
                cancel_tasks.append(self._cancel_single_order(slice_order_id, symbol, f"{purpose}_TWAP_SLICE_CANCEL"))
        
        await asyncio.gather(*cancel_tasks, return_exceptions=True)
        
        self.twap_orders.pop(main_order_id, None)
        LOGGER.info(f"TWAPAlgo: All slices for TWAP {main_order_id} cancelled. TWAP order removed.")
        # Emit final OrderUpdate for the main order, status 'canceled'
        final_order_obj = twap_info["main_order_request"].data
        final_order_obj["status"] = "canceled"
        final_order_obj["filled"] = twap_info["filled_amount"] # Report any partial fills
        final_order_obj["remaining"] = pov_info["total_amount"] - pov_info["filled_amount"]
        await self._emit_order_update_event(final_order_obj, f"{purpose}_TWAP_CANCELLED", twap_info["pos_id"])

class POVAlgo(ExecutionAlgo):
    """
    Percentage of Volume (POV) algorithm.
    Places orders to participate in a fixed percentage of market volume over time.
    More adaptive than TWAP.
    [Refonte: POV]
    """
    def __init__(self, exchange_client: Any, event_queue: asyncio.Queue, config: Dict):
        super().__init__(exchange_client, event_queue, config)
        self.pov_orders: Dict[str, Dict] = {} # {main_order_id: {target_pct_of_volume, total_amount, filled_amount, ...}}
        self.market_data_update_interval_seconds = 1 # How often to check market volume
        
    async def execute_order(self, order_request: OrderRequestEvent) -> Optional[Dict]:
        main_client_order_id = order_request.client_order_id
        symbol = order_request.symbol
        side = order_request.side
        total_amount = order_request.amount
        limit_price = order_request.price
        purpose = order_request.purpose
        pos_id = order_request.position_id
        
        algo_params = order_request.algo_params
        target_pct_of_volume = algo_params.get("target_pct_of_volume", 0.05) # Default 5% of market volume
        max_slice_amount_pct = algo_params.get("max_slice_amount_pct", 0.2) # Max 20% of total order per slice

        if total_amount <= 1e-9:
            self.logger.warning(f"POVAlgo: Total amount for POV is zero or too small for {symbol}. Order not placed.")
            return None

        self.pov_orders[main_client_order_id] = {
            "symbol": symbol, "side": side, "total_amount": total_amount, 
            "filled_amount": 0.0, "limit_price": limit_price, "purpose": purpose, "pos_id": pos_id,
            "target_pct_of_volume": target_pct_of_volume, "max_slice_amount_pct": max_slice_amount_pct,
            "is_completed": False, "main_order_request": order_request
        }
        
        self.logger.info(f"POVAlgo: Initiating POV for {symbol} {side} {total_amount:.8f} at {target_pct_of_volume*100:.2f}% participation.")
        
        asyncio.create_task(
            self._pov_slicing_task(main_client_order_id, symbol, side, total_amount, limit_price, purpose, pos_id, target_pct_of_volume, max_slice_amount_pct, order_request.params),
            name=f"POV_Slicing_{main_client_order_id[:8]}"
        )
        return {"id": main_client_order_id, "symbol": symbol, "type": "POV", "status": "open", "amount": total_amount, "filled": 0.0, "remaining": total_amount}

    async def _pov_slicing_task(self, main_client_order_id: str, symbol: str, side: str, total_amount: float, limit_price: Optional[float], purpose: str, pos_id: str, target_pct_of_volume: float, max_slice_amount_pct: float, base_params: Dict):
        pov_info = self.pov_orders[main_client_order_id]

        while self._running and not pov_info["is_completed"]:
            remaining_amount = total_amount - pov_info["filled_amount"]
            if remaining_amount <= 1e-9:
                pov_info["is_completed"] = True
                self.logger.info(f"POVAlgo: POV for {main_client_order_id[:8]} completed. All amount filled.")
                break

            # Get current market volume (from recent ticks or OHLCV)
            async with STATE_LOCK:
                # Use a recent 1-minute OHLCV volume as a proxy for market volume
                ohlcv_1m_df = SHARED_STATE["ohlcv_data"][symbol].get("1m")
                current_market_volume_1m = ohlcv_1m_df['volume'].iloc[-1] if ohlcv_1m_df is not None and not ohlcv_1m_df.empty else 0.0
                current_price = self._get_current_price_from_data_cache(symbol) or (limit_price if limit_price else 0)

            if current_market_volume_1m <= 1e-9 or current_price <= 1e-9:
                self.logger.warning(f"POVAlgo: Market volume or price for {symbol} unavailable. Waiting for data.")
                await asyncio.sleep(self.market_data_update_interval_seconds)
                continue
            
            # Estimate how much volume we want to participate with in this interval
            target_volume_to_trade_base_ccy = (current_market_volume_1m * target_pct_of_volume) 
            
            # Limit the size of each slice to prevent large single orders (e.g., max 20% of remaining total)
            max_slice_amount = total_amount * max_slice_amount_pct
            current_slice_amount = min(target_volume_to_trade_base_ccy, remaining_amount, max_slice_amount)

            current_slice_amount = adjust_amount_for_order(symbol, current_slice_amount, self.exchange_client)
            if current_slice_amount <= 1e-9:
                self.logger.debug(f"POVAlgo: Calculated slice amount for {symbol} is too small ({current_slice_amount:.8f}). Waiting for next interval.")
                await asyncio.sleep(self.market_data_update_interval_seconds)
                continue

            slice_client_order_id = f"{main_client_order_id}_pov_slice_{get_current_utc_ms()}"
            slice_params = base_params.copy()
            slice_params["clientOrderId"] = slice_client_order_id
            
            if self.trading_config["use_reduce_only"] and self.exchange_client.options.get('defaultType') == 'future' and purpose.startswith(("TAKE_PROFIT", "STOP_LOSS", "COMMAND_FORCE_CLOSE", "EMERGENCY_CLOSE")):
                slice_params["reduceOnly"] = True

            # Decide on order type for slice (e.g., limit at target_price, or market for urgency)
            # For POV, usually a LIMIT order near BBO to be passive, but could be MARKET if participation is key
            order_type_for_slice = "limit" if limit_price is not None else "market"
            order_price_for_slice = limit_price # If limit, use the overall limit price

            self.logger.debug(f"POVAlgo: Placing POV slice for {symbol}: {current_slice_amount:.8f} as {order_type_for_slice} @ {order_price_for_slice:.5f if order_price_for_slice else 'Market'}")
            
            slice_order = await self._place_single_order(symbol, order_type_for_slice, side, current_slice_amount, order_price_for_slice, slice_params)
            
            if slice_order:
                self.active_orders[slice_order["id"]] = slice_order # Track individual slice order
                
                if slice_order["status"] == "filled":
                    pov_info["filled_amount"] += to_float(slice_order.get("filled", 0.0))
                    await self._emit_order_update_event(slice_order, f"{purpose}_POV_SLICE_FILLED", pos_id)
                else:
                    asyncio.create_task(self._monitor_slice_fill(slice_order["id"], symbol, purpose, pos_id, main_client_order_id), name=f"POV_SliceMonitor_{slice_order['id'][:8]}")

            else:
                self.logger.error(f"POVAlgo: Failed to place POV slice for {symbol}. Stopping POV slicing for {main_client_order_id[:8]}.")
                break # Stop slicing if a slice fails

            await asyncio.sleep(self.market_data_update_interval_seconds) # Wait for next market data update interval

        # After loop, handle final status of the main POV order
        pov_info["is_completed"] = True
        if pov_info["filled_amount"] >= total_amount * 0.999:
            LOGGER.info(f"POVAlgo: POV for {main_client_order_id[:8]} completed. Total filled: {pov_info['filled_amount']:.8f}/{total_amount:.8f}.")
            final_order_obj = pov_info["main_order_request"].data
            final_order_obj["status"] = "filled"
            final_order_obj["filled"] = pov_info["filled_amount"]
            final_order_obj["remaining"] = pov_info["total_amount"] - pov_info["filled_amount"]
            await self._emit_order_update_event(final_order_obj, f"{purpose}_POV_FINAL", pos_id)
        else:
            LOGGER.warning(f"POVAlgo: POV for {main_client_order_id[:8]} finished, but only partially filled: {pov_info['filled_amount']:.8f}/{total_amount:.8f}.")
            final_order_obj = pov_info["main_order_request"].data
            final_order_obj["status"] = "partial_fill" if pov_info["filled_amount"] > 0 else "canceled"
            final_order_obj["filled"] = pov_info["filled_amount"]
            final_order_obj["remaining"] = pov_info["total_amount"] - pov_info["filled_amount"]
            await self._emit_order_update_event(final_order_obj, f"{purpose}_POV_PARTIAL_OR_FAILED", pos_id)

    async def _monitor_slice_fill(self, slice_order_id: str, symbol: str, purpose: str, pos_id: str, main_client_order_id: str):
        # Similar logic to TWAP's slice monitor, checking status and updating main order's filled amount.
        timeout = self.execution_config["order_fill_timeout_seconds"]
        start_time = time.time()

        while self._running and (time.time() - start_time) < timeout:
            async with STATE_LOCK:
                order_in_shared_state = SHARED_STATE["open_orders"].get(slice_order_id)
            if order_in_shared_state and order_in_shared_state['status'] in ['open', 'partial']:
                await asyncio.sleep(self.execution_config["order_status_check_interval_seconds"])
            elif order_in_shared_state and order_in_shared_state['status'] == 'filled':
                pov_info = self.pov_orders.get(main_client_order_id)
                if pov_info:
                    current_filled_by_slice = to_float(order_in_shared_state.get('filled', 0.0))
                    pov_info["filled_amount"] += current_filled_by_slice
                    LOGGER.debug(f"POVAlgo: Slice {slice_order_id[:8]} for {symbol} filled. Total POV filled: {pov_info['filled_amount']:.8f}.")
                break
            else: 
                if order_in_shared_state: LOGGER.warning(f"POVAlgo: Slice {slice_order_id[:8]} for {symbol} finished with status: {order_in_shared_state['status']}.")
                else: LOGGER.warning(f"POVAlgo: Slice {slice_order_id[:8]} for {symbol} disappeared from tracking.")
                break
        if not self._running or (time.time() - start_time) >= timeout:
            LOGGER.warning(f"POVAlgo: Slice {slice_order_id[:8]} for {symbol} timed out after {timeout}s or bot stopping. Attempting to cancel.")
            await self._cancel_single_order(slice_order_id, symbol, f"POV_SLICE_TIMEOUT_CANCEL")


    async def cancel_order(self, main_order_id: str, symbol: str, purpose: str):
        """
        Annule une commande principale de type POV et toutes ses sous-commandes (slices) actives.
        """
        # 1. La variable `pov_info` est définie ICI, au début de la méthode.
        #    C'est la correction la plus importante.
        pov_info = self.pov_orders.get(main_order_id)
        
        # Si la commande n'est pas trouvée, on quitte la fonction pour éviter les erreurs.
        if not pov_info:
            LOGGER.warning(f"POVAlgo: Impossible d'annuler la commande POV {main_order_id} pour {symbol}. Non trouvée.")
            return

        LOGGER.info(f"POVAlgo: Annulation de la commande POV {main_order_id} et de ses sous-commandes pour {symbol}.")
        
        pov_info["is_completed"] = True  # Signal pour arrêter la création de nouvelles sous-commandes
        cancel_tasks = []
        
        # Recherche et annulation de toutes les sous-commandes encore ouvertes
        async with STATE_LOCK:
            slices_to_cancel_ids = [oid for oid, odata in SHARED_STATE["open_orders"].items() if odata.get("clientOrderId", "").startswith(f"{main_order_id}_pov_slice_")]
        
        for slice_order_id in slices_to_cancel_ids:
            cancel_tasks.append(self._cancel_single_order(slice_order_id, symbol, f"{purpose}_POV_SLICE_CANCEL"))
        
        await asyncio.gather(*cancel_tasks, return_exceptions=True)
        
        self.pov_orders.pop(main_order_id, None)
        LOGGER.info(f"POVAlgo: Toutes les sous-commandes pour POV {main_order_id} ont été annulées.")
        
        # Préparation de l'événement final pour la commande principale
        final_order_obj = pov_info["main_order_request"].data
        final_order_obj["status"] = "canceled"
        final_order_obj["filled"] = pov_info["filled_amount"]
        
        # 2. Maintenant que `pov_info` est bien défini, cette ligne (votre ligne 3958) est correcte.
        final_order_obj["remaining"] = pov_info["total_amount"] - pov_info["filled_amount"]
        
        await self._emit_order_update_event(final_order_obj, f"{purpose}_POV_CANCELLED", pov_info["pos_id"])


class IcebergAlgo(ExecutionAlgo):
    """
    Iceberg algorithm. Places a large limit order by revealing only a small "tip"
    to the market, and replenishing it as it gets filled. Reduces market impact and
    reveals less intent.
    [Refonte: Iceberg]
    """
    def __init__(self, exchange_client: Any, event_queue: asyncio.Queue, config: Dict):
        super().__init__(exchange_client, event_queue, config)
        self.iceberg_orders: Dict[str, Dict] = {} # {main_order_id: {total_amount, visible_amount, filled_amount, current_slice_id, ...}}
        self.default_visible_amount_pct = 0.1 # 10% of remaining total amount visible
        self.reposition_delay_seconds = 0.5 # Delay before repositioning a new tip

    async def execute_order(self, order_request: OrderRequestEvent) -> Optional[Dict]:
        main_client_order_id = order_request.client_order_id
        symbol = order_request.symbol
        side = order_request.side
        total_amount = order_request.amount
        limit_price = order_request.price
        purpose = order_request.purpose
        pos_id = order_request.position_id
        
        algo_params = order_request.algo_params
        visible_amount_pct = algo_params.get("visible_amount_pct", self.default_visible_amount_pct)
        
        if total_amount <= 1e-9 or limit_price is None:
            self.logger.warning(f"IcebergAlgo: Total amount or limit price invalid for Iceberg {symbol}. Order not placed.")
            return None

        self.iceberg_orders[main_client_order_id] = {
            "symbol": symbol, "side": side, "total_amount": total_amount, 
            "filled_amount": 0.0, "limit_price": limit_price, "purpose": purpose, "pos_id": pos_id,
            "visible_amount_pct": visible_amount_pct,
            "current_slice_order_id": None, # The ID of the currently active 'tip' order
            "is_completed": False, "main_order_request": order_request
        }
        
        self.logger.info(f"IcebergAlgo: Initiating Iceberg for {symbol} {side} {total_amount:.8f} @ {limit_price:.5f} with {visible_amount_pct*100:.2f}% tip.")
        
        asyncio.create_task(
            self._iceberg_slicing_task(main_client_order_id, symbol, side, limit_price, purpose, pos_id, order_request.params),
            name=f"Iceberg_Slicing_{main_client_order_id[:8]}"
        )
        return {"id": main_client_order_id, "symbol": symbol, "type": "ICEBERG", "status": "open", "amount": total_amount, "filled": 0.0, "remaining": total_amount}

    async def _iceberg_slicing_task(self, main_client_order_id: str, symbol: str, side: str, limit_price: float, purpose: str, pos_id: str, base_params: Dict):
        iceberg_info = self.iceberg_orders.get(main_client_order_id)
        if not iceberg_info:
            return

        while self._running and not iceberg_info.get("is_completed", True):
            remaining_amount = iceberg_info["total_amount"] - iceberg_info["filled_amount"]
            if remaining_amount <= 1e-9:
                iceberg_info["is_completed"] = True
                self.logger.info(f"IcebergAlgo: Iceberg for {main_client_order_id[:8]} completed. All amount filled.")
                break

            current_slice_order_id = iceberg_info["current_slice_order_id"]
            if current_slice_order_id:
                async with STATE_LOCK:
                    slice_order_status = SHARED_STATE["open_orders"].get(current_slice_order_id, {}).get('status')
                
                if slice_order_status == "filled":
                    iceberg_info["current_slice_order_id"] = None
                    await asyncio.sleep(self.reposition_delay_seconds)
                    continue
                elif slice_order_status in ["canceled", "rejected", "expired"]:
                    iceberg_info["current_slice_order_id"] = None
                    await asyncio.sleep(self.reposition_delay_seconds)
                    continue
                elif slice_order_status in ["open", "partial"]:
                    await asyncio.sleep(self.execution_config["order_status_check_interval_seconds"])
                    continue
                else:
                    iceberg_info["current_slice_order_id"] = None
                    await asyncio.sleep(self.reposition_delay_seconds)
                    continue
            
            slice_amount = min(remaining_amount, iceberg_info["total_amount"] * iceberg_info["visible_amount_pct"])
            slice_amount = adjust_amount_for_order(symbol, slice_amount, self.exchange_client)
            
            if slice_amount <= 1e-9:
                iceberg_info["is_completed"] = True
                break

            slice_client_order_id = f"{main_client_order_id}_iceberg_slice_{get_current_utc_ms()}"
            slice_params = base_params.copy()
            slice_params["clientOrderId"] = slice_client_order_id

            if self.trading_config["use_reduce_only"] and self.exchange_client and self.exchange_client.options.get('defaultType') == 'future' and purpose.startswith(("TAKE_PROFIT", "STOP_LOSS", "COMMAND_FORCE_CLOSE", "EMERGENCY_CLOSE")):
                slice_params["reduceOnly"] = True
            
            slice_order = await self._place_single_order(symbol, "limit", side, slice_amount, limit_price, slice_params)
            
            if slice_order:
                self.active_orders[slice_order["id"]] = slice_order
                iceberg_info["current_slice_order_id"] = slice_order["id"]
                asyncio.create_task(self._monitor_slice_fill(slice_order["id"], symbol, purpose, pos_id, main_client_order_id), name=f"Iceberg_SliceMonitor_{slice_order['id'][:8]}")
            else:
                break

        final_iceberg_info = self.iceberg_orders.get(main_client_order_id, iceberg_info)
        if final_iceberg_info["filled_amount"] >= final_iceberg_info["total_amount"] * 0.999:
            final_order_obj = final_iceberg_info["main_order_request"].data
            final_order_obj["status"] = "filled"
            final_order_obj["filled"] = final_iceberg_info["filled_amount"]
            final_order_obj["remaining"] = final_iceberg_info["total_amount"] - final_iceberg_info["filled_amount"]
            await self._emit_order_update_event(final_order_obj, f"{purpose}_ICEBERG_FINAL", pos_id)
        else:
            final_order_obj = final_iceberg_info["main_order_request"].data
            final_order_obj["status"] = "partial_fill" if final_iceberg_info["filled_amount"] > 0 else "canceled"
            final_order_obj["filled"] = final_iceberg_info["filled_amount"]
            final_order_obj["remaining"] = final_iceberg_info["total_amount"] - final_iceberg_info["filled_amount"]
            await self._emit_order_update_event(final_order_obj, f"{purpose}_ICEBERG_PARTIAL_OR_FAILED", pos_id)

    async def _monitor_slice_fill(self, slice_order_id: str, symbol: str, purpose: str, pos_id: str, main_client_order_id: str):
        timeout = self.execution_config["order_fill_timeout_seconds"]
        start_time = time.time()

        while self._running and (time.time() - start_time) < timeout:
            async with STATE_LOCK:
                order_in_shared_state = SHARED_STATE["open_orders"].get(slice_order_id)
            
            iceberg_info = self.iceberg_orders.get(main_client_order_id)
            if not iceberg_info: break
            
            if order_in_shared_state and order_in_shared_state['status'] in ['open', 'partial']:
                current_filled_by_slice = to_float(order_in_shared_state.get('filled', 0.0))
                new_filled_amount = current_filled_by_slice - iceberg_info.get("last_reported_slice_filled", 0.0)
                if new_filled_amount > 0:
                    iceberg_info["filled_amount"] += new_filled_amount
                    iceberg_info["last_reported_slice_filled"] = current_filled_by_slice
                await asyncio.sleep(self.execution_config["order_status_check_interval_seconds"])
            elif order_in_shared_state and order_in_shared_state['status'] == 'filled':
                final_filled_by_slice = to_float(order_in_shared_state.get('filled', 0.0))
                new_filled_amount = final_filled_by_slice - iceberg_info.get("last_reported_slice_filled", 0.0)
                if new_filled_amount > 0:
                    iceberg_info["filled_amount"] += new_filled_amount
                iceberg_info["current_slice_order_id"] = None
                break
            else:
                iceberg_info["current_slice_order_id"] = None
                break
        
        if not self._running or (time.time() - start_time) >= timeout:
            iceberg_info = self.iceberg_orders.get(main_client_order_id)
            if iceberg_info and iceberg_info["current_slice_order_id"] == slice_order_id:
                await self._cancel_single_order(slice_order_id, symbol, "ICEBERG_SLICE_TIMEOUT_CANCEL")

    async def cancel_order(self, main_order_id: str, symbol: str, purpose: str):
        iceberg_info = self.iceberg_orders.get(main_order_id)
        if not iceberg_info:
            return

        iceberg_info["is_completed"] = True
        
        if iceberg_info["current_slice_order_id"]:
            await self._cancel_single_order(iceberg_info["current_slice_order_id"], symbol, f"{purpose}_ICEBERG_TIP_CANCEL")
        
        self.iceberg_orders.pop(main_order_id, None)
        
        final_order_obj = iceberg_info["main_order_request"].data
        final_order_obj["status"] = "canceled"
        final_order_obj["filled"] = iceberg_info["filled_amount"]
        final_order_obj["remaining"] = iceberg_info["total_amount"] - iceberg_info["filled_amount"]
        await self._emit_order_update_event(final_order_obj, f"{purpose}_ICEBERG_CANCELLED", iceberg_info["pos_id"])

class ExecutionHandler:
    """
    ExecutionHandler: The phantom executor. Executes large orders discreetly
    with minimal cost and market impact using a suite of execution algorithms.
    Monitors API latency actively.
    [Refonte: Suite d'algorithmes d'exécution, Surveillance Latence]
    """
    def __init__(self, event_queue: asyncio.Queue, config: Dict):
        self.event_queue = event_queue
        self.config = config
        self.execution_config = config["execution"]
        self.trading_config = config["trading"]
        self.exchange_config = config["exchange"]

        self._running = False
        self.exchange_client: Optional[ccxt.Exchange] = None
        self.ccxt_config = {
            'apiKey': BINANCE_API_KEY,
            'secret': BINANCE_API_SECRET,
            'timeout': self.exchange_config["rest_api_timeout_seconds"] * 1000,
            'options': {
                'defaultType': 'future' if self.trading_config["leverage"] > 1 else 'spot',
                'recvWindow': 15000, 
                'adjustForTimeDifference': True,
                'newOrderRespType': 'FULL',
                'warnOnFetchOpenOrdersWithoutSymbol': False, # <--- AJOUTEZ CETTE LIGNE
            }
        }
        if self.exchange_config["sandbox_mode"]:
            self.ccxt_config['urls'] = {'api': 'https://testnet.binancefuture.com/fapi'}
            LOGGER.warning("ExecutionHandler: Sandbox mode ACTIVE. Connecting to testnet.")
        
        self.order_monitor_interval = self.execution_config["rest_reconciliation_interval_seconds"]
        self.last_order_monitor_time = 0.0
        self.order_status_check_interval = self.execution_config["order_status_check_interval_seconds"]

        # Circuit breaker for general exchange API calls
        self.exchange_api_circuit_breaker = CircuitBreaker(
            failure_threshold=5, recovery_timeout=300, logger=LOGGER
        )
        
        # [Refonte: Gestionnaires d'Algo] Map algorithm names to their instances
        self.execution_algos: Dict[str, ExecutionAlgo] = {
            "DEFAULT_MARKET": DefaultMarketAlgo(None, self.event_queue, self.config), # Client assigned in run()
            "PASSIVE_LIMIT": PassiveLimitAlgo(None, self.event_queue, self.config),
            "TWAP": TWAPAlgo(None, self.event_queue, self.config),
            "POV": POVAlgo(None, self.event_queue, self.config),
            "ICEBERG": IcebergAlgo(None, self.event_queue, self.config),
            # Add other algos here
        }

        self.last_api_latency_check_time = 0.0
        self.api_latency_check_interval = 5 # Check API latency every 5 seconds

        LOGGER.info("ExecutionHandler initialized with execution algorithms.")

    async def run(self):
        self._running = True
        LOGGER.info("ExecutionHandler starting. Connecting to exchange...")
        
        exchange_class = getattr(ccxt, self.exchange_config["name"])
        self.exchange_client = exchange_class(self.ccxt_config)
        
        if self.exchange_config["sandbox_mode"] and hasattr(self.exchange_client, 'set_sandbox_mode'):
            self.exchange_client.set_sandbox_mode(True)
        self.exchange_client.enableRateLimit = True # Uses CCXT's built-in rate limit manager for REST API calls
        
        # Pass the exchange client instance to all execution algorithms
        for algo_name, algo_instance in self.execution_algos.items():
            algo_instance.exchange_client = self.exchange_client

        try:
            await self.exchange_api_circuit_breaker(self.exchange_client.load_markets)()
            LOGGER.info(f"ExecutionHandler: Connected to {self.exchange_client.name}. Markets loaded.")
            # Initial order reconciliation on startup to synchronize state
            await self._reconcile_open_orders() 
        except Exception as e:
            LOGGER.critical(f"ExecutionHandler: Failed to connect to exchange: {e}. Cannot operate.", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("CRITICAL", f"EXH: Exchange connection failed. Cannot operate."))
            self._running = False
            return

        # Start background tasks
        self._order_monitoring_task = asyncio.create_task(self._monitor_open_orders_loop(), name="EXH_OrderMonitor")
        #self._api_latency_monitor_task = asyncio.create_task(self._monitor_api_latency_loop(), name="EXH_APILatencyMonitor")
        
        # Main loop for processing order requests from event queue
        while self._running:
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0) # Small timeout to check _running flag
                if event.type == "ORDER_REQUEST":
                    await self.on_order_request_event(event)
                else:
                    LOGGER.debug(f"ExecutionHandler: Received unexpected event type: {event.type}. Ignoring.")
                self.event_queue.task_done()
            except asyncio.TimeoutError:
                pass # No events, continue loop
            except asyncio.CancelledError:
                LOGGER.info("ExecutionHandler: Main loop cancelled.")
                break
            except Exception as e:
                LOGGER.error(f"ExecutionHandler: Unhandled error in main loop: {e}", exc_info=True)
                await self.event_queue.put(SystemAlertEvent("ERROR", f"EXH: Unhandled error in main loop: {e}"))
                await asyncio.sleep(1) # Prevent rapid error loop on continuous errors

        LOGGER.info("ExecutionHandler stopped.")

    async def on_order_request_event(self, event: OrderRequestEvent):
        """
        Processes an order request (place, cancel, modify, emergency close).
        Delegates the execution to the appropriate execution algorithm.
        """
        if not self._running:
            LOGGER.warning(f"ExecutionHandler: Order request {event.request_type} for {event.symbol} ignored. Handler is stopped.")
            return

        request_data = event.data
        request_type = request_data["request_type"]
        purpose = request_data["purpose"]
        symbol = request_data["symbol"]
        pos_id = request_data.get("position_id")
        
        # Check global kill switch (redundant, but good for safety)
        async with STATE_LOCK:
            if SHARED_STATE["kill_switch_active"] and purpose not in ["EMERGENCY_CLOSE_SHUTDOWN", "CANCEL_ALL_OPEN_ORDERS_ON_EXCHANGE"]:
                LOGGER.warning(f"ExecutionHandler: Order request {request_type} {purpose} for {symbol} ignored. Global Kill Switch Active.")
                # Emit a rejected update for this order
                await self._emit_order_update_event(
                    {"id": "N/A", "symbol": symbol, "status": "rejected", "amount": request_data["amount"]}, 
                    purpose, pos_id, client_order_id=request_data.get("client_order_id", "N/A"),
                    reason="KillSwitchActive"
                )
                return

        try:
            if request_type == "CREATE":
                algo_type = request_data["algo_params"].get("algo_type", "DEFAULT_MARKET")
                algo = self.execution_algos.get(algo_type)
                if not algo:
                    LOGGER.error(f"ExecutionHandler: Unknown execution algorithm type requested: {algo_type} for {symbol}. Order not placed.")
                    await self._emit_order_update_event(
                        {"id": "N/A", "symbol": symbol, "status": "rejected", "amount": request_data["amount"]}, 
                        purpose, pos_id, client_order_id=request_data.get("client_order_id", "N/A"),
                        reason="UnknownAlgo"
                    )
                    return
                
                # Check and set leverage first (if applicable, only for new entries)
                if purpose.startswith("ENTRY_"):
                    await self._set_leverage_for_symbol(symbol, request_data["params"].get("leverage"))
                
                await algo.execute_order(event) # Delegate to the chosen algorithm

            elif request_type == "CANCEL":
                await self._cancel_order(request_data)
            elif request_type == "MODIFY":
                await self._modify_order(request_data)
            elif request_type == "CANCEL_ALL_FOR_POS":
                await self._cancel_all_orders_for_position(request_data["position_id"], request_data["symbol"])
            elif request_type == "CANCEL_ALL_OPEN_ORDERS_ON_EXCHANGE": 
                await self._cancel_all_open_orders_on_exchange()
            else:
                LOGGER.warning(f"ExecutionHandler: Unknown order request type: {request_type}.")

        except CircuitBreakerOpenError:
            LOGGER.warning(f"ExecutionHandler: Circuit breaker OPEN. Skipping order request {request_type} {purpose}.")
            await self._emit_order_update_event(
                {"id": "N/A", "symbol": symbol, "status": "rejected", "amount": request_data["amount"]}, 
                purpose, pos_id, client_order_id=request_data.get("client_order_id", "N/A"),
                reason="CircuitBreakerOpen"
            )
        except Exception as e:
            LOGGER.error(f"ExecutionHandler: Error processing order request {request_type} {purpose} for {symbol}: {e}", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("ERROR", f"EXH: Error processing {request_type} {purpose}: {e}"))
            await self._emit_order_update_event(
                {"id": "N/A", "symbol": symbol, "status": "rejected", "amount": request_data["amount"]}, 
                purpose, pos_id, client_order_id=request_data.get("client_order_id", "N/A"),
                reason=f"UnhandledError: {e}"
            )

    async def _set_leverage_for_symbol(self, symbol: str, desired_leverage: Optional[int]):
        """
        Sets the leverage for a given symbol on the exchange.
        Avoids redundant API calls if leverage is already set.
        """
        if not desired_leverage or not hasattr(self.exchange_client, 'set_leverage'):
            return # Not applicable or not supported by exchange

        try:
            # Fetch current leverage for the symbol
            current_leverage = None
            positions = await self.exchange_api_circuit_breaker(self.exchange_client.fetch_positions)(symbols=[symbol])
            if positions:
                current_leverage = to_int(positions[0].get('leverage'))
            
            if current_leverage is None or current_leverage != desired_leverage:
                await self.exchange_api_circuit_breaker(self.exchange_client.set_leverage)(desired_leverage, symbol)
                LOGGER.info(f"ExecutionHandler: Leverage set to {desired_leverage}x for {symbol}.")
                async with STATE_LOCK:
                    SHARED_STATE["telemetry_data"][f"leverage_{symbol.replace('/','')}"] = desired_leverage
            else:
                LOGGER.debug(f"ExecutionHandler: Leverage for {symbol} already set to {desired_leverage}x. No change needed.")
        except Exception as e:
            LOGGER.warning(f"ExecutionHandler: Failed to set leverage {desired_leverage}x for {symbol}: {e}. This might impact trading if wrong leverage is applied.", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("WARNING", f"EXH: Failed to set leverage {desired_leverage}x for {symbol}: {e}"))

    async def _emit_order_update_event(self, order: Dict, purpose: str, pos_id: str, client_order_id: Optional[str] = None, reason: Optional[str] = None):
        """
        Emits an OrderUpdateEvent based on an order object from the exchange.
        Centralizes the update logic for all algorithms.
        """
        if not order:
            LOGGER.error(f"ExecutionHandler: Attempted to emit update for null order object. Purpose: {purpose}, PosID: {pos_id}.")
            return
        
        status = order.get('status', 'unknown').lower()
        filled_amount = to_float(order.get('filled', 0.0))
        remaining_amount = to_float(order.get('remaining', order.get('amount', 0.0)) - filled_amount)
        fill_price = to_float(order.get('average', order.get('price', 0.0)))
        fee_cost = to_float(order.get('fee', {}).get('cost', 0.0))
        fee_currency = order.get('fee', {}).get('currency', self.trading_config["capital_currency"])
        
        # Add a 'reason' to order_details if provided, useful for rejected/cancelled states
        order_details_with_reason = order.copy()
        if reason:
            order_details_with_reason['reason'] = reason

        update_event = OrderUpdateEvent(
            exchange_order_id=order.get("id", "N/A"),
            client_order_id=client_order_id or order.get("clientOrderId", "N/A"),
            symbol=order.get("symbol", "UNKNOWN"),
            status=status,
            filled_amount=filled_amount,
            remaining_amount=remaining_amount,
            fill_price=fill_price,
            purpose=purpose,
            position_id=pos_id or order.get("info", {}).get("position_id", "N/A"), # Try to retrieve pos_id from order info
            fee=fee_cost,
            fee_currency=fee_currency,
            order_details=order_details_with_reason
        )
        await self.event_queue.put(update_event)
        LOGGER.debug(f"EXH: Emitted OrderUpdateEvent for {order.get('id', 'N/A')} ({order.get('symbol', 'UNKNOWN')}) - Status: {status}, Filled: {filled_amount}.")
        
        # Remove order from internal tracking if final status
        if status in ['filled', 'canceled', 'rejected', 'expired']:
            async with STATE_LOCK:
                SHARED_STATE["open_orders"].pop(order.get("id"), None)
                SHARED_STATE["client_order_map"].pop(client_order_id or order.get("clientOrderId"), None)


    async def _monitor_open_orders_loop(self):
        """
        Background loop to periodically reconcile the bot's internal open orders
        with those reported by the exchange. This catches orders missed by WS updates.
        """
        while self._running:
            await asyncio.sleep(self.order_monitor_interval)
            await self._reconcile_open_orders()
            async with STATE_LOCK:
                SHARED_STATE["telemetry_data"]["order_reconciliations"] = SHARED_STATE["telemetry_data"].get("order_reconciliations", 0) + 1

    @retry_async(max_retries=CONFIG["execution"]["cancel_order_retries"], delay_seconds=CONFIG["execution"]["cancel_order_retry_delay_seconds"], catch_exceptions=(ccxt.NetworkError, ccxt.ExchangeError, ccxt.ExchangeNotAvailable))
    @CircuitBreaker(failure_threshold=5, recovery_timeout=60, logger=LOGGER)
    async def _reconcile_open_orders(self):
        """
        Reconciles the bot's internal list of open orders with the actual orders on the exchange.
        Updates internal state and emits OrderUpdateEvents for any discrepancies.
        """
        LOGGER.debug("ExecutionHandler: Reconciling open orders with exchange via REST API.")
        try:
            # 1. Fetch all currently open orders from the exchange
            exchange_orders = await self.exchange_client.fetch_open_orders()
            # Convert the list to a dictionary for efficient O(1) lookup by order ID
            exchange_orders_map = {order['id']: order for order in exchange_orders}
            
            async with STATE_LOCK:
                # 2. Get a snapshot of currently tracked orders by the bot.
                # We iterate over a copy of the keys because the dictionary may be modified during the loop.
                bot_orders_to_check = list(SHARED_STATE["open_orders"].keys())

                for order_id in bot_orders_to_check:
                    # Retrieve the bot's version of the order safely
                    bot_order = SHARED_STATE["open_orders"].get(order_id)
                    if not bot_order:
                        # The order might have been removed by another process concurrently. Safe to skip.
                        continue

                    # Check if the order still exists in the exchange's list of open orders
                    exchange_order = exchange_orders_map.get(order_id)

                    if exchange_order:
                        # CASE 1: The order is still open on the exchange.
                        # We check if its state (e.g., filled amount) has changed without the bot knowing.
                        if exchange_order['status'] != bot_order['status'] or \
                           to_float(exchange_order['filled']) != to_float(bot_order['filled']):
                            
                            LOGGER.info(f"Reconciliation: Discrepancy found for order {order_id}. Bot Status: {bot_order['status']}, Exchange Status: {exchange_order['status']}. Bot Filled: {to_float(bot_order['filled'])}, Exchange Filled: {to_float(exchange_order['filled'])}.")
                            
                            # Update the bot's internal state with the authoritative version from the exchange
                            SHARED_STATE["open_orders"][order_id] = exchange_order
                            
                            # Emit an OrderUpdateEvent to notify other modules (like PortfolioRiskManager) of the change
                            await self._emit_order_update_event(
                                exchange_order, 
                                bot_order.get("info", {}).get("purpose", "RECONCILED_UPDATE"), 
                                bot_order.get("info", {}).get("position_id", "N/A"),
                                client_order_id=bot_order.get("clientOrderId")
                            )
                    else:
                        # CASE 2: The order is in the bot's state but NOT found on the exchange.
                        # This is a "ghost order". It was either filled, canceled, or rejected, and the bot missed the update.
                        # This is a critical state to correct.
                        LOGGER.warning(f"Reconciliation: Ghost order detected. Bot order {order_id} ({bot_order['symbol']}) is not open on the exchange. Fetching its final status.")
                        
                        try:
                            # The most robust way to handle this is to fetch the order's history directly
                            final_order_status = await self.exchange_client.fetch_order(order_id, bot_order['symbol'])
                            LOGGER.info(f"Reconciliation: Final status for ghost order {order_id} is '{final_order_status['status']}'. Updating internal state.")
                            
                            # Emit an event with the final, correct status. This will trigger the removal of the order
                            # from SHARED_STATE["open_orders"] inside _emit_order_update_event.
                            await self._emit_order_update_event(
                                final_order_status,
                                bot_order.get("info", {}).get("purpose", "RECONCILED_FINAL_STATUS"),
                                bot_order.get("info", {}).get("position_id", "N/A"),
                                client_order_id=bot_order.get("clientOrderId")
                            )
                        except ccxt.OrderNotFound:
                            # If even fetch_order can't find it, the order is truly gone (e.g., very old).
                            # The safest assumption is that it was canceled, as assuming a fill could incorrectly
                            # alter a position's state.
                            LOGGER.warning(f"Reconciliation: Ghost order {order_id} ({bot_order['symbol']}) could not be fetched. Assuming it was implicitly canceled/expired.")
                            
                            # Create a fake 'canceled' order object to emit a final update
                            bot_order['status'] = 'canceled'
                            await self._emit_order_update_event(
                                bot_order,
                                bot_order.get("info", {}).get("purpose", "RECONCILED_IMPLICIT_CANCEL"),
                                bot_order.get("info", {}).get("position_id", "N/A"),
                                client_order_id=bot_order.get("clientOrderId")
                            )
                        except Exception as fetch_e:
                            # If fetching the final status fails for other reasons (e.g., temporary API error),
                            # we log it but don't change the state yet to avoid making a wrong assumption.
                            LOGGER.error(f"Reconciliation: Failed to fetch final status for ghost order {order_id} ({bot_order['symbol']}): {fetch_e}. Will retry in the next cycle.", exc_info=True)
                            
        except CircuitBreakerOpenError:
            LOGGER.warning("ExecutionHandler: Circuit Breaker is OPEN for exchange API. Skipping order reconciliation cycle.")
            await self.event_queue.put(HealthCheckEvent("ExchangeAPI", "warning", "Circuit Breaker OPEN. Order reconciliation skipped."))
        except Exception as e:
            LOGGER.error(f"ExecutionHandler: An unexpected error occurred during order reconciliation: {e}", exc_info=True)
            await self.event_queue.put(SystemAlertEvent("ERROR", f"EXH: Critical error during order reconciliation: {e}"))

# PARTIE 4/5 : L'ORCHESTRATEUR PRINCIPAL (BOTCONTROLLER) ET LE MONITORING

# ==============================================================================
# SECTION 4: L'ORCHESTRATEUR PRINCIPAL (BOT CONTROLLER)
# ==============================================================================
class UIManager:
    def __init__(self, event_queue: asyncio.Queue, config: Dict):
        self.logger = LOGGER
        self.alert_config = config["monitoring"]

    async def run(self):
        self.logger.info("UIManager en mode logging uniquement.")
        # Cette classe ne fait rien en mode simple, elle existe pour la compatibilité

    async def stop(self):
        self.logger.info("UIManager arrêté.")

    async def on_position_update_event(self, event: PositionUpdateEvent):
        self.logger.info(f"UI_UPDATE: Position {event.symbol} {event.update_type} | Détails: {event.details}")

    async def on_system_alert_event(self, event: SystemAlertEvent):
        self.logger.info(f"UI_ALERT [{event.level}]: {event.message} | Détails: {event.details}")

    async def on_health_check_event(self, event: HealthCheckEvent):
        self.logger.info(f"UI_HEALTH [{event.level}]: Composant '{event.component}' statut {event.status}. Message: {event.message}")

class BotController:
    def __init__(self, mode: str = "live"):
        self.mode = mode
        self.logger = LOGGER # Use the global logger
        
        # Initialize the global event queue
        SHARED_STATE["event_queue"] = asyncio.Queue()
        self.event_queue: asyncio.Queue = SHARED_STATE["event_queue"]

        self.modules: Dict[str, Any] = {} # Store module instances
        self.module_tasks: List[asyncio.Task] = [] # Tasks for each module's run loop
        self._main_loop_task: Optional[asyncio.Task] = None # Task for the main event dispatch loop

        self._initialize_modules() # Instantiate all modules

        
        SHARED_STATE["is_running"] = True # Mark as starting
        SHARED_STATE["system_status"] = "INITIALIZING"
        
        self.last_ntp_sync_time = 0.0
        self.ntp_sync_interval_seconds = 3600 # Sync NTP every hour
        self.last_health_check_report_time = 0.0
        self.health_check_report_interval_seconds = 60 # Report health every minute
        self.last_telemetry_push_time = 0.0
        self.telemetry_push_interval_seconds = 1 # Push metrics every second

        self.logger.info("BotController initialized.")

    def _initialize_modules(self):
        """Instantiates all core modules and registers them."""
        self.logger.info("BotController: Initializing core modules...")
        
        # Order of instantiation matters for dependencies
        self.modules["MarketDataGateway"] = MarketDataGateway(CONFIG)
        self.modules["FeatureEngine"] = FeatureEngine(CONFIG)
        self.modules["AlphaModel"] = AlphaModel(self.event_queue, CONFIG)
        self.modules["PortfolioRiskManager"] = PortfolioRiskManager(self.event_queue, CONFIG)
        self.modules["ExecutionHandler"] = ExecutionHandler(self.event_queue, CONFIG)
        self.modules["UIManager"] = UIManager(self.event_queue, CONFIG)

    async def run(self):
        self.logger.info("BotController: Starting execution.")
        
        # Perform initial NTP synchronization
        await self._synchronize_ntp_time()

        # Start all module run loops as asyncio tasks
        for name, module in self.modules.items():
            if hasattr(module, 'run'):
                task = asyncio.create_task(module.run(), name=f"ModuleTask-{name}")
                self.module_tasks.append(task)
                self.logger.info(f"BotController: Launched task for module {name}.")

        # Start the main event dispatch loop
        self._main_loop_task = asyncio.create_task(self._event_dispatch_loop(), name="MainEventDispatchLoop")
        
        # Start background health monitoring and telemetry pushing
        self._health_monitoring_task = asyncio.create_task(self._health_monitoring_loop(), name="HealthMonitoringLoop")
        self._telemetry_push_task = asyncio.create_task(self._telemetry_push_loop(), name="TelemetryPushLoop")

        async with STATE_LOCK:
            SHARED_STATE["system_status"] = "LIVE_TRADING"
            self.logger.info("BotController: System status set to LIVE_TRADING.")
            # Initial alert confirming bot startup
            await self.event_queue.put(SystemAlertEvent("INFO", f"KRONOS Bot {CONFIG['system']['instance_name']} started in {self.mode.upper()} mode. Initial capital: {SHARED_STATE['global_capital']['initial']:.2f} {CONFIG['trading']['capital_currency']}"))

        # Gather all main tasks, running them concurrently
        all_tasks = [self._main_loop_task, self._health_monitoring_task, self._telemetry_push_task] + self.module_tasks
        try:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            self.logger.info("BotController: All main tasks were cancelled.")
        except Exception as e:
            self.logger.critical(f"BotController: Unhandled exception in main asyncio.gather: {e}", exc_info=True)
            await self.shutdown() # Attempt graceful shutdown on unhandled error

        self.logger.info("BotController: All main tasks have completed.")

    async def _event_dispatch_loop(self):
        self.logger.info("BotController: Event dispatch loop started.")
        loop_interval_ms = CONFIG["system"]["main_loop_interval_ms"]
        
        while SHARED_STATE["is_running"]:
            loop_start_time = time.time()
            current_utc_ms = get_current_utc_ms()
            async with STATE_LOCK:
                SHARED_STATE["loop_counter"] += 1
                SHARED_STATE["last_loop_timestamp"] = loop_start_time
                SHARED_STATE["current_utc_ms"] = current_utc_ms
                is_kill_switch_active = SHARED_STATE["kill_switch_active"]
                system_status = SHARED_STATE["system_status"]

            # Periodically synchronize NTP time
            if time.time() - self.last_ntp_sync_time > self.ntp_sync_interval_seconds:
                await self._synchronize_ntp_time()
                self.last_ntp_sync_time = time.time()

            events_processed_this_loop = 0
            # Process events from the queue until it's empty or loop time limit is reached
            while True:
                try:
                    # Get event from queue with a small timeout to allow checking _running flag
                    event = await asyncio.wait_for(self.event_queue.get(), timeout=0.01) 
                    
                    if is_kill_switch_active:
                        # In kill switch mode, only process critical events
                        if event.type not in ["ORDER_UPDATE", "SYSTEM_ALERT", "BOT_COMMAND", "HEALTH_CHECK"]:
                            LOGGER.debug(f"BotController: Event {event.type} ignored. Kill Switch Active.")
                            self.event_queue.task_done()
                            continue
                    
                    await self._dispatch_event_to_modules(event)
                    events_processed_this_loop += 1
                    self.event_queue.task_done()

                    # Prevent a single loop from taking too long if event queue is very large
                    if (time.time() - loop_start_time) * 1000 > loop_interval_ms * 0.8: # Use 80% of interval
                        # LOGGER.debug(f"BotController: Event dispatch loop taking too long ({events_processed_this_loop} events). Yielding.")
                        break # Break to allow next loop iteration

                except asyncio.TimeoutError:
                    # Queue is empty, break to yield control and ensure loop interval is respected
                    break 
                except asyncio.CancelledError:
                    LOGGER.info("BotController: Event dispatch loop cancelled.")
                    return
                except Exception as e:
                    LOGGER.critical(f"BotController: CRITICAL ERROR in event dispatch loop: {e}", exc_info=True)
                    async with STATE_LOCK:
                        SHARED_STATE["is_running"] = False
                        SHARED_STATE["system_status"] = "ERROR_MAIN_LOOP_CRASH"
                    await self.event_queue.put(SystemAlertEvent("CRITICAL", f"BotController: CRITICAL DISPATCHER ERROR: {e}. Shutting down."))
                    return # Exit loop on critical error

            # Ensure the loop runs approximately at main_loop_interval_ms
            elapsed_time_ms = (time.time() - loop_start_time) * 1000
            sleep_time_ms = max(0, loop_interval_ms - elapsed_time_ms)
            if sleep_time_ms > 0:
                await asyncio.sleep(sleep_time_ms / 1000)

        LOGGER.info("BotController: Event dispatch loop finished.")

    async def _dispatch_event_to_modules(self, event: Event):
        """Dispatches an event to the appropriate module's handler method."""
        try:
            if event.type == "MARKET_DATA_UPDATE":
                # DataHandler handles its internal processing. This event is for others.
                # AlphaModel might consume this if it calculates features on-the-fly directly.
                # Here, AlphaModel consumes FeatureEvent, so MARKET_DATA_UPDATE is not directly consumed by it.
                pass 
            elif event.type == "FEATURE_UPDATE":
                await self.modules["AlphaModel"].on_feature_event(event)
            elif event.type == "SIGNAL":
                await self.modules["PortfolioRiskManager"].on_signal_event(event)
            elif event.type == "ORDER_REQUEST":
                await self.modules["ExecutionHandler"].on_order_request_event(event)
            elif event.type == "ORDER_UPDATE":
                await self.modules["PortfolioRiskManager"].on_order_update_event(event) # PRM processes fills/status changes
            elif event.type == "POSITION_UPDATE":
                # PRM emits this, UIManager consumes it for alerts/dashboard
                await self.modules["UIManager"].on_position_update_event(event)
            elif event.type == "SYSTEM_ALERT":
                await self.modules["UIManager"].on_system_alert_event(event) # UIManager's specific handler
            elif event.type == "BOT_COMMAND":
                await self._process_bot_command(event)
            elif event.type == "HEALTH_CHECK":
                await self.modules["UIManager"].on_health_check_event(event) # UIManager handles health alerts
            elif event.type == "METRIC_UPDATE":
                # This is primarily for pushing to Prometheus exporter
                pass 
            else:
                LOGGER.warning(f"BotController: Unhandled event type in dispatcher: {event.type}.")
        except Exception as e:
            LOGGER.error(f"BotController: Error dispatching event {event.type} to module: {e}", exc_info=True)
            # Do not re-raise to keep the dispatch loop running

    async def _process_bot_command(self, event: BotCommandEvent):
        """Handles internal bot commands (e.g., kill switch, pause strategy)."""
        command = event.command
        target = event.target
        value = event.value
        
        LOGGER.info(f"BotController: Processing command: {command} (Target: {target}, Value: {value})")

        async with STATE_LOCK:
            if command == "ACTIVATE_GLOBAL_KILL_SWITCH":
                SHARED_STATE["kill_switch_active"] = True
                SHARED_STATE["system_status"] = "PAUSED_KILL_SWITCH"
                LOGGER.critical("BotController: GLOBAL KILL SWITCH ACTIVATED by command. Operations suspended.")
                await self.event_queue.put(SystemAlertEvent("CRITICAL", "BotController: GLOBAL KILL SWITCH ACTIVATED by command."))
                # Trigger emergency actions: close all positions, cancel all orders
                await self._emergency_close_all_positions_and_orders()
            elif command == "DEACTIVATE_GLOBAL_KILL_SWITCH":
                 if SHARED_STATE["kill_switch_active"]:
                    SHARED_STATE["kill_switch_active"] = False
                    SHARED_STATE["system_status"] = "LIVE_TRADING" if not SHARED_STATE["health_checks"]["api_status"] == "unavailable" else "HEALTH_WARNING"
                    LOGGER.warning("BotController: GLOBAL KILL SWITCH DEACTIVATED by command.")
                    await self.event_queue.put(SystemAlertEvent("WARNING", "BotController: GLOBAL KILL SWITCH DEACTIVATED."))
                 else:
                    LOGGER.info("BotController: Global Kill Switch not active. Command ignored.")
            elif command == "PAUSE_STRATEGY":
                if target in SHARED_STATE["live_strategy_parameters"]:
                    SHARED_STATE["live_strategy_parameters"][target]["enabled"] = False
                    LOGGER.warning(f"BotController: Strategy {target} PAUSED by command.")
                    await self.event_queue.put(SystemAlertEvent("WARNING", f"BotController: Strategy {target} PAUSED."))
            elif command == "RESUME_STRATEGY":
                if target in SHARED_STATE["live_strategy_parameters"]:
                    SHARED_STATE["live_strategy_parameters"][target]["enabled"] = True
                    LOGGER.info(f"BotController: Strategy {target} RESUMED by command.")
                    await self.event_queue.put(SystemAlertEvent("INFO", f"BotController: Strategy {target} RESUMED."))
            elif command == "FORCE_CLOSE_POSITION":
                if target in SHARED_STATE["active_positions"]:
                    position_to_close = SHARED_STATE["active_positions"][target]
                    await self.event_queue.put(OrderRequestEvent(
                        request_type="CREATE",
                        symbol=position_to_close["symbol"],
                        order_type="MARKET", # Force market close
                        side="SELL" if position_to_close["side"] == "BUY" else "BUY",
                        amount=position_to_close["current_amount"],
                        price=None,
                        purpose="COMMAND_FORCE_CLOSE",
                        position_id=target
                    ))
                    LOGGER.warning(f"BotController: Force close command issued for position {target}.")
                    await self.event_queue.put(SystemAlertEvent("WARNING", f"BotController: Force closing {target}."))
                else:
                    LOGGER.warning(f"BotController: Force close command for position {target} ignored (not found).")
            elif command == "SET_LEVERAGE":
                symbol = target
                leverage_value = to_int(value)
                if symbol and leverage_value > 0:
                    # Send an internal request to ExecutionHandler to set leverage
                    # (This is more of a config change or pre-trade action)
                    LOGGER.info(f"BotController: Setting leverage for {symbol} to {leverage_value}x via command (future implementation).")
                    await self.modules["ExecutionHandler"].set_leverage_for_symbol(symbol, leverage_value) # Needs to be implemented
                else:
                    LOGGER.warning(f"BotController: Invalid SET_LEVERAGE command: symbol={symbol}, value={value}.")
            else:
                LOGGER.warning(f"BotController: Unknown bot command: {command}.")

    async def _emergency_close_all_positions_and_orders(self):
        """Initiates an emergency shutdown sequence: cancels all orders, closes all positions."""
        LOGGER.critical("BotController: Starting EMERGENCY SHUTDOWN PROCEDURE (positions & orders).")
        # 1. Cancel all open orders first to prevent them from interfering with position closure
        await self.modules["ExecutionHandler"]._cancel_all_open_orders_on_exchange()
        
        # 2. Close all active positions
        async with STATE_LOCK:
            positions_to_close = list(SHARED_STATE["active_positions"].keys()) 
        
        if not positions_to_close:
            LOGGER.info("BotController: No active positions to emergency close.")
            return

        for pos_id in positions_to_close:
            async with STATE_LOCK:
                position = SHARED_STATE["active_positions"].get(pos_id)
            if position and position["status"] == "open":
                await self.event_queue.put(OrderRequestEvent(
                    request_type="CREATE",
                    symbol=position["symbol"],
                    order_type="MARKET", # Force market close
                    side="SELL" if position["side"] == "BUY" else "BUY",
                    amount=position["current_amount"],
                    price=None,
                    purpose="EMERGENCY_CLOSE_SHUTDOWN",
                    position_id=pos_id
                ))
                LOGGER.critical(f"BotController: Emergency close request sent for position {pos_id}.")
            elif position:
                LOGGER.warning(f"BotController: Position {pos_id} not open ({position['status']}). Ignored for emergency close.")
            else:
                LOGGER.warning(f"BotController: Position {pos_id} not found in active state. Ignored for emergency close.")
        
        LOGGER.critical("BotController: All EMERGENCY CLOSE requests submitted. Monitor exchange directly!")

    # ==========================================================================
    # SYSTEM HEALTH & TELEMETRY MONITORING
    # ==========================================================================
    async def _synchronize_ntp_time(self):
        """Synchronizes system time with NTP server for high precision."""
        try:
            import ntplib
            client = ntplib.NTPClient()
            response = await asyncio.to_thread(client.request, CONFIG["system"]["ntp_server"], version=3)
            # Calculate offset from system time
            offset_seconds = response.offset
            
            if abs(offset_seconds) > 0.1: # If offset is greater than 100ms
                LOGGER.warning(f"BotController: NTP time offset detected: {offset_seconds:.3f} seconds. Adjusting system time (requires root/admin).")
                # Attempt to adjust system time (platform specific and requires permissions)
                # For most scenarios, just logging and using this offset internally is enough.
                # On Linux: os.system(f"sudo date -s '@{time.time() + offset_seconds}'")
                
                # For bot's internal timestamps, can add this offset.
                # However, usually exchange API syncs timestamp, so this is more for local system clock drift.
                async with STATE_LOCK:
                    SHARED_STATE["telemetry_data"]["ntp_offset_seconds"] = offset_seconds
                    SHARED_STATE["telemetry_data"]["last_ntp_sync_timestamp"] = time.time()
            else:
                LOGGER.debug(f"BotController: NTP time is synchronized (offset: {offset_seconds:.3f}s).")

            self.last_ntp_sync_time = time.time()

        except ImportError:
            LOGGER.warning("BotController: 'ntplib' not installed. Skipping NTP time synchronization. (pip install ntplib)")
        except Exception as e:
            LOGGER.error(f"BotController: Failed to synchronize NTP time: {e}", exc_info=True)
            await self.event_queue.put(HealthCheckEvent("NTP_Sync", "warning", f"NTP sync failed: {e}"))

    async def _health_monitoring_loop(self):
        """Periodically collects and reports system health metrics."""
        while SHARED_STATE["is_running"]:
            await asyncio.sleep(self.health_check_report_interval_seconds)
            await self._perform_health_checks()
            async with STATE_LOCK:
                SHARED_STATE["telemetry_data"]["health_checks_performed"] = SHARED_STATE["telemetry_data"].get("health_checks_performed", 0) + 1

    async def _perform_health_checks(self):
        """Performs various internal health checks."""
        LOGGER.debug("BotController: Performing internal health checks.")
        
        # 1. Check module status (if their run loops are still active)
        for task in self.module_tasks:
            if task.done():
                exception = task.exception()
                if exception:
                    LOGGER.critical(f"BotController: Module task {task.get_name()} crashed with exception: {exception}. Shutting down bot.", exc_info=True)
                    await self.event_queue.put(SystemAlertEvent("CRITICAL", f"Module {task.get_name()} crashed: {exception}. Shutting down."))
                    async with STATE_LOCK:
                        SHARED_STATE["is_running"] = False
                        SHARED_STATE["system_status"] = "ERROR_MODULE_CRASH"
                    return # Critical error, trigger shutdown

        # 2. Check event queue size
        queue_size = self.event_queue.qsize()
        if queue_size > 1000: # Threshold for alert
            LOGGER.warning(f"BotController: Event queue size is high: {queue_size}. Possible processing bottleneck.")
            await self.event_queue.put(HealthCheckEvent("EventQueue", "warning", f"Queue size high: {queue_size}"))
        
        # 3. Check API status (via circuit breaker status from ExecutionHandler/PRM)
        # This is implicitly handled by CircuitBreaker's state
        # The circuit breaker itself will emit HealthCheckEvents

        # 4. Check data freshness
        async with STATE_LOCK:
            current_time = get_current_utc_ms()
            for symbol in CONFIG["exchange"]["active_symbols"]:
                # Check freshness of 1s OHLCV
                ohlcv_1s_df = SHARED_STATE["ohlcv_data"][symbol].get("1s")
                if ohlcv_1s_df is not None and not ohlcv_1s_df.empty:
                    last_candle_time_ms = ohlcv_1s_df.index[-1].timestamp() * 1000
                    if (current_time - last_candle_time_ms) > (timeframe_to_seconds("1s") * 1000 * 5): # 5 seconds without 1s candle
                        LOGGER.warning(f"BotController: Data gap detected for {symbol} 1s OHLCV. Last candle {last_candle_time_ms}.")
                        SHARED_STATE["health_checks"]["data_gaps_detected"][f"{symbol}_1s"] = True
                        await self.event_queue.put(HealthCheckEvent("DataGap", "warning", f"Data gap for {symbol} 1s OHLCV."))
                    else:
                        SHARED_STATE["health_checks"]["data_gaps_detected"][f"{symbol}_1s"] = False
                
                # Check freshness of order book
                ob_snapshot = SHARED_STATE["order_book_snapshots"][symbol]
                if ob_snapshot.get("timestamp", 0) > 0 and (current_time - ob_snapshot["timestamp"]) > 5000: # 5 seconds without OB update
                    LOGGER.warning(f"BotController: Order book for {symbol} is stale. Last update {ob_snapshot['timestamp']}.")
                    SHARED_STATE["health_checks"]["data_gaps_detected"][f"OB_{symbol}"] = True
                    await self.event_queue.put(HealthCheckEvent("DataGap", "warning", f"Stale Order Book for {symbol}."))
                else:
                    SHARED_STATE["health_checks"]["data_gaps_detected"][f"OB_{symbol}"] = False

        # 5. Check memory and CPU usage (requires psutil)
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            rss_mb = mem_info.rss / (1024 * 1024)
            cpu_percent = process.cpu_percent(interval=1) # Measures CPU usage since last call or 1s interval

            async with STATE_LOCK:
                SHARED_STATE["telemetry_data"]["memory_usage_mb"] = rss_mb
                SHARED_STATE["telemetry_data"]["cpu_usage_percent"] = cpu_percent
            
            if rss_mb > CONFIG["system"]["max_memory_usage_mb"]:
                LOGGER.critical(f"BotController: High Memory Usage: {rss_mb:.2f} MB. Exceeds limit {CONFIG['system']['max_memory_usage_mb']} MB.")
                await self.event_queue.put(SystemAlertEvent("CRITICAL", f"BotController: High Memory Usage: {rss_mb:.2f} MB."))
            if cpu_percent > CONFIG["system"]["cpu_usage_alert_threshold_pct"]:
                LOGGER.warning(f"BotController: High CPU Usage: {cpu_percent:.2f}%.")
                await self.event_queue.put(SystemAlertEvent("WARNING", f"BotController: High CPU Usage: {cpu_percent:.2f}%."))

        except ImportError:
            LOGGER.debug("BotController: 'psutil' not installed. Skipping memory/CPU monitoring.")
        except Exception as e:
            LOGGER.error(f"BotController: Error getting memory/CPU usage: {e}")

        LOGGER.debug("BotController: Health checks completed.")
        self.last_health_check_report_time = time.time()

    async def _telemetry_push_loop(self):
        """Periodically pushes telemetry data for external monitoring (e.g., Prometheus)."""
        while SHARED_STATE["is_running"]:
            await asyncio.sleep(self.telemetry_push_interval_seconds)
            await self._push_telemetry_metrics()
            async with STATE_LOCK:
                SHARED_STATE["telemetry_data"]["metrics_pushed_count"] = SHARED_STATE["telemetry_data"].get("metrics_pushed_count", 0) + 1

    async def _push_telemetry_metrics(self):
        """Pushes current state and performance metrics to SHARED_STATE['telemetry_data']."""
        async with STATE_LOCK:
            current_telemetry = SHARED_STATE["telemetry_data"]
            
            # General system metrics
            current_telemetry["system_status"] = SHARED_STATE["system_status"]
            current_telemetry["is_running"] = SHARED_STATE["is_running"]
            current_telemetry["kill_switch_active"] = SHARED_STATE["kill_switch_active"]
            current_telemetry["event_queue_size"] = self.event_queue.qsize()
            current_telemetry["alert_queue_size"] = SHARED_STATE["alert_queue"].qsize()
            current_telemetry["open_orders_count"] = len(SHARED_STATE["open_orders"])
            current_telemetry["active_positions_count"] = len(SHARED_STATE["active_positions"])

            # Capital and PnL
            current_telemetry["global_capital_total"] = SHARED_STATE["global_capital"]["total"]
            current_telemetry["global_capital_available"] = SHARED_STATE["global_capital"]["available"]
            current_telemetry["global_capital_initial"] = SHARED_STATE["global_capital"]["initial"]
            current_telemetry["global_capital_peak"] = SHARED_STATE["global_capital"]["peak"]
            current_telemetry["global_capital_max_drawdown_pct"] = SHARED_STATE["global_capital"]["max_drawdown_pct"]
            current_telemetry["global_capital_daily_pnl"] = SHARED_STATE["global_capital"]["daily_pnl"]
            current_telemetry["global_capital_daily_fees"] = SHARED_STATE["global_capital"]["daily_fees"]
            current_telemetry["lifetime_realized_pnl"] = SHARED_STATE["global_capital"]["lifetime_realized_pnl"]
            current_telemetry["lifetime_fees_paid"] = SHARED_STATE["global_capital"]["lifetime_fees_paid"]

            # Position-specific metrics (for active positions)
            for pos_id, pos_data in SHARED_STATE["active_positions"].items():
                pos_labels = {"symbol": pos_data["symbol"], "side": pos_data["side"], "strategy": pos_data["strategy_id"]}
                current_telemetry[f"position_pnl_usd_{pos_id[:8]}"] = {"value": pos_data["pnl_usd"], "labels": pos_labels}
                current_telemetry[f"position_pnl_pct_{pos_id[:8]}"] = {"value": pos_data["pnl_percentage"], "labels": pos_labels}
                current_telemetry[f"position_current_amount_{pos_id[:8]}"] = {"value": pos_data["current_amount"], "labels": pos_labels}
                current_telemetry[f"position_entry_price_{pos_id[:8]}"] = {"value": pos_data["entry_price"], "labels": pos_labels}
                current_telemetry[f"position_liquidation_price_{pos_id[:8]}"] = {"value": pos_data["liquidation_price"], "labels": pos_labels}

            # Strategy performance metrics
            for strat_id, strat_perf in SHARED_STATE["strategy_performance_metrics"].items():
                strat_labels = {"strategy_id": strat_id}
                current_telemetry[f"strategy_consecutive_losses_{strat_id}"] = {"value": strat_perf["consecutive_losses"], "labels": strat_labels}
                current_telemetry[f"strategy_daily_pnl_usd_{strat_id}"] = {"value": strat_perf["daily_pnl_usd"], "labels": strat_labels}
                current_telemetry[f"strategy_win_rate_{strat_id}"] = {"value": strat_perf["win_rate"], "labels": strat_labels}
                current_telemetry[f"strategy_total_trades_{strat_id}"] = {"value": strat_perf["total_trades"], "labels": strat_labels}
                current_telemetry[f"strategy_profit_factor_{strat_id}"] = {"value": strat_perf["profit_factor"], "labels": strat_labels}
                current_telemetry[f"strategy_alpha_decay_score_{strat_id}"] = {"value": strat_perf["alpha_decay_score"], "labels": strat_labels}
                current_telemetry[f"strategy_status_{strat_id}"] = {"value": 1 if strat_perf["current_status"] == "active" else 0, "labels": strat_labels}

            # Data stream health
            for stream_uri, status in SHARED_STATE["health_checks"]["websocket_status"].items():
                stream_labels = {"stream_uri": stream_uri}
                current_telemetry[f"websocket_connection_status"] = {"value": 1 if status == "connected" else 0, "labels": stream_labels}
            
            # API status
            current_telemetry["api_status_ok"] = 1 if SHARED_STATE["health_checks"]["api_status"] == "ok" else 0
            
            # Memory & CPU (if psutil is active)
            # These metrics are already put into current_telemetry by _perform_health_checks
            
            SHARED_STATE["telemetry_data"]["last_metrics_pushed"] = time.time()
            # The exporter (in main.py) will read this SHARED_STATE["telemetry_data"]

    async def shutdown(self):
        """Gracefully shuts down the bot and all its components."""
        async with STATE_LOCK:
            if SHARED_STATE["system_status"] == "SHUTDOWN":
                LOGGER.info("BotController: Shutdown sequence already completed.")
                return
            
            SHARED_STATE["is_running"] = False
            SHARED_STATE["system_status"] = "SHUTDOWN_INITIATED"
            LOGGER.critical("BotController: Global shutdown signal initiated.")

        # Cancel main tasks first
        if self._main_loop_task: self._main_loop_task.cancel()
        if self._health_monitoring_task: self._health_monitoring_task.cancel()
        if self._telemetry_push_task: self._telemetry_push_task.cancel()
        
        # Give a small grace period for tasks to acknowledge cancellation
        await asyncio.sleep(0.5)

        # Orderly shutdown of modules
        LOGGER.info("BotController: Shutting down modules in a controlled sequence.")
        # Stop modules in reverse order of dependency (approximate)
        shutdown_order = ["UIManager", "ExecutionHandler", "PortfolioRiskManager", "AlphaModel", "DataHandler"]
        
        tasks_to_await_shutdown = []
        for name in shutdown_order:
            module = self.modules.get(name)
            if module and hasattr(module, 'stop'):
                LOGGER.info(f"BotController: Calling stop() on module {name}.")
                tasks_to_await_shutdown.append(asyncio.create_task(module.stop()))
        
        # Wait for all module stop tasks to complete
        await asyncio.gather(*tasks_to_await_shutdown, return_exceptions=True)

        # Final cleanup actions
        if CONFIG["trading"]["close_all_on_shutdown"] and not SHARED_STATE["kill_switch_active"]:
            LOGGER.info("BotController: Attempting final closure of positions and cancellation of orders as per config.")
            await self._emergency_close_all_positions_and_orders() # This might submit orders
            # Give a small delay for any last orders to be submitted/processed
            await asyncio.sleep(1) 
        else:
            LOGGER.info("BotController: Automatic position/order cleanup on shutdown disabled or kill switch active. Manual check advised.")
        
        # Ensure all event queues are empty
        while not self.event_queue.empty():
            try: self.event_queue.get_nowait()
            except asyncio.QueueEmpty: break
        while not SHARED_STATE["alert_queue"].empty():
            try: SHARED_STATE["alert_queue"].get_nowait()
            except asyncio.QueueEmpty: break

        LOGGER.critical("BotController: --- KRONOS Monolith Bot SHUTDOWN COMPLETE ---")
        logging.shutdown() # Ensure all logs are flushed and files closed

# PARTIE 5/5 : LE LABORATOIRE DE RECHERCHE & POINT D'ENTRÉE PRINCIPAL (BRAIN & INTERFACE)

# ==============================================================================
# SECTION 5: LE LABORATOIRE DE RECHERCHE (BACKTESTING ET ENTRAÎNEMENT)
# ==============================================================================

class ResearchLab:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = LOGGER
        self.historical_data_path = config["research"]["historical_data_path"]
        self.model_save_dir = config["research"]["model_save_dir"]
        os.makedirs(self.model_save_dir, exist_ok=True)

        self.feature_scaler: Optional[StandardScaler] = None
        self.garch_models = defaultdict(dict) 
        
        self.load_ml_assets()

    def load_ml_assets(self):
        scaler_path = self.config["research"]["feature_scaler_save_path"]
        if os.path.exists(scaler_path):
            try:
                self.feature_scaler = joblib.load(scaler_path)
                self.logger.info(f"ResearchLab: Feature scaler loaded from {scaler_path}.")
            except Exception as e:
                self.logger.error(f"ResearchLab: Error loading feature scaler: {e}")
        
        garch_model_path = self.config["research"]["garch_model_save_path"]
        if os.path.exists(garch_model_path):
            try:
                loaded_garch_models = joblib.load(garch_model_path)
                for symbol, tf_models in loaded_garch_models.items():
                    for tf, model in tf_models.items():
                        self.garch_models[symbol][tf] = model
                        self.logger.info(f"ResearchLab: GARCH model for {symbol} {tf} loaded.")
            except Exception as e:
                self.logger.error(f"ResearchLab: Error loading GARCH models: {e}")

    def _load_raw_tick_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        MODIFIÉ POUR LE BACKTEST: Charge les données de bougies de 1 minute et les simule comme des ticks.
        """
        clean_symbol = symbol.replace('/', '').replace(':', '').upper()
        # On pointe vers le nouveau fichier que vous avez téléchargé
        filepath = os.path.join(self.historical_data_path, f"{clean_symbol}_1m_data.csv")
        
        if not os.path.exists(filepath):
            self.logger.error(f"ResearchLab: Fichier de données de 1 minute non trouvé: {filepath}.")
            raise FileNotFoundError(f"Missing 1m data file for {symbol}.")

        self.logger.info(f"ResearchLab: Chargement des données de 1 minute pour {symbol} depuis {filepath}...")
        
        try:
            # Charger tout le fichier d'un coup, car il est de taille raisonnable
            df = pd.read_csv(filepath)
            
            # Convertir la colonne 'timestamp' en objets datetime et la définir comme index
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df.set_index('timestamp', inplace=True)
            
            # Filtrer par date
            df_filtered = df[(df.index >= pd.Timestamp(start_date, tz='UTC')) & 
                             (df.index <= pd.Timestamp(end_date, tz='UTC'))]
            
            if df_filtered.empty:
                self.logger.warning(f"ResearchLab: Aucune donnée trouvée dans la plage de dates {start_date}-{end_date}.")
                return pd.DataFrame()

            # --- SIMULATION DES TICKS ---
            # On transforme les bougies en un format qui ressemble à des ticks pour le reste du code
            # On prend le prix de clôture 'close' comme prix du tick
            # Et le volume de la minute comme volume du tick
            # 'is_buyer_maker' est simulé : True si la bougie est baissière, False si haussière
            df_simulated_ticks = pd.DataFrame({
                'p': df_filtered['close'],
                'volume': df_filtered['volume'],
                'm': df_filtered['close'] < df_filtered['open']
            })

            self.logger.info(f"ResearchLab: {len(df_simulated_ticks)} bougies de 1m chargées et simulées comme des ticks.")
            return df_simulated_ticks

        except Exception as e:
            self.logger.error(f"ResearchLab: Erreur lors du chargement des données de 1m depuis {filepath}: {e}", exc_info=True)
            return pd.DataFrame()

    def _reconstruct_ohlcv_from_ticks(self, tick_df: pd.DataFrame, timeframe_seconds: int) -> pd.DataFrame:
        """Reconstructs OHLCV candles from raw tick data."""
        if tick_df.empty: return pd.DataFrame()
        
        rule = f"{timeframe_seconds}S" if timeframe_seconds < 60 else f"{timeframe_seconds // 60}Min" # For pandas resampling
        
        # Ensure 'p' is price and 'volume' is quantity
        ohlcv_df = tick_df['p'].resample(rule).ohlc()
        ohlcv_df['volume'] = tick_df['volume'].resample(rule).sum()
        
        ohlcv_df.ffill(inplace=True) 
        ohlcv_df.bfill(inplace=True) 
        ohlcv_df.dropna(inplace=True) 
        
        return ohlcv_df

    def _calculate_all_features_for_df(self, ohlcv_df_dict: Dict[str, pd.DataFrame], tick_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Calculates all features on historical DataFrames."""
        self.logger.info(f"ResearchLab: Calculating historical features for {symbol}...")
        
        # Base DataFrame for features will be 1-minute candles
        if "1m" not in ohlcv_df_dict or ohlcv_df_dict["1m"].empty:
            self.logger.error(f"ResearchLab: 1m OHLCV data required for feature calculation for {symbol}.")
            return pd.DataFrame()
        
        features_df = pd.DataFrame(index=ohlcv_df_dict["1m"].index)

        # Order Flow Imbalance (OFI) on historical ticks
        # This part requires iterating over ticks and grouping by minute or feature interval
        # For simplicity in backtest feature generation, we approximate OFI on 1m candles
        if not ohlcv_df_dict["1m"].empty:
            ohlcv_1m_copy_for_ofi = ohlcv_df_dict["1m"].copy()
            # Simple OFI approximation based on candle direction
            ohlcv_1m_copy_for_ofi['change'] = ohlcv_1m_copy_for_ofi['close'].diff()
            ohlcv_1m_copy_for_ofi['buy_vol'] = ohlcv_1m_copy_for_ofi.apply(lambda row: row['volume'] if row['change'] > 0 else 0, axis=1)
            ohlcv_1m_copy_for_ofi['sell_vol'] = ohlcv_1m_copy_for_ofi.apply(lambda row: row['volume'] if row['change'] < 0 else 0, axis=1)
            
            # Calculate rolling OFI for different periods (e.g., 10, 30 candles)
            for window in [10, 30]:
                rolling_buy_vol = ohlcv_1m_copy_for_ofi['buy_vol'].rolling(window=window).sum()
                rolling_sell_vol = ohlcv_1m_copy_for_ofi['sell_vol'].rolling(window=window).sum()
                total_rolling_vol = rolling_buy_vol + rolling_sell_vol
                features_df[f'OFI_{window}m_candle'] = (rolling_buy_vol - rolling_sell_vol) / total_rolling_vol
                features_df[f'OFI_{window}m_candle'].fillna(0, inplace=True)
            
            # Simple OFI for last 100 ticks (requires reconstruction of tick-based OFI)
            # This is complex in batch. A simplified version could use resampled tick data
            # For backtesting, often a tick-level simulation is needed for true OFI features
            # Here, we will map a candle-based OFI to "OFI_100_tick" as a placeholder
            features_df['OFI_100_tick'] = features_df['OFI_10m_candle'] # Placeholder: use 10m candle OFI as tick OFI

        # VWAP Deviation
        if "5m" in ohlcv_df_dict and not ohlcv_df_dict["5m"].empty:
            ohlcv_5m_copy_for_vwap = ohlcv_df_dict["5m"].copy()
            ohlcv_5m_copy_for_vwap['tpv'] = ohlcv_5m_copy_for_vwap['close'] * ohlcv_5m_copy_for_vwap['volume']
            ohlcv_5m_copy_for_vwap['cum_tpv'] = ohlcv_5m_copy_for_vwap['tpv'].expanding().sum()
            ohlcv_5m_copy_for_vwap['cum_vol'] = ohlcv_5m_copy_for_vwap['volume'].expanding().sum()
            ohlcv_5m_copy_for_vwap['vwap'] = ohlcv_5m_copy_for_vwap['cum_tpv'] / ohlcv_5m_copy_for_vwap['cum_vol']
            
            features_df['VWAP_Dev_5m'] = (ohlcv_5m_copy_for_vwap['close'] - ohlcv_5m_copy_for_vwap['vwap']) / ohlcv_5m_copy_for_vwap['vwap']
            features_df['VWAP_Dev_5m'].fillna(0, inplace=True)
            features_df['VWAP_Dev_5m'] = features_df['VWAP_Dev_5m'].reindex(features_df.index, method='ffill')
        else: features_df['VWAP_Dev_5m'] = 0.0
        
        # Momentum (Slope)
        for period_mins in [10, 60]:
            tf_str = f"{period_mins}m"
            if tf_str in ohlcv_df_dict and not ohlcv_df_dict[tf_str].empty:
                df_tf_for_momentum = ohlcv_df_dict[tf_str].copy()
                window = 20 # Candles for slope calculation
                
                # Pre-calculate slopes (avoids lambda in apply for performance)
                slopes = df_tf_for_momentum['close'].rolling(window=window).apply(
                    lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) == window else np.nan, raw=True
                )
                df_tf_for_momentum[f'Momentum_{period_mins}m_slope'] = slopes
                features_df[f'Momentum_{period_mins}m_slope'] = df_tf_for_momentum[f'Momentum_{period_mins}m_slope'].reindex(features_df.index, method='ffill')
                features_df[f'Momentum_{period_mins}m_slope'].fillna(0, inplace=True)
            else: features_df[f'Momentum_{period_mins}m_slope'] = 0.0

        # Volatility (GARCH)
        if "1h" in ohlcv_df_dict and not ohlcv_df_dict["1h"].empty and len(ohlcv_df_dict["1h"]) > 100:
            df_1h_for_garch = ohlcv_df_dict["1h"].copy()
            returns_1h = 100 * df_1h_for_garch['close'].pct_change().dropna()
            
            if len(returns_1h) > 50:
                try:
                    if symbol not in self.garch_models or "1h" not in self.garch_models[symbol]:
                        # Re-train GARCH model if not loaded (for walk-forward first iteration)
                        p, q = self.config["research"]["garch_p_q_params"]
                        model_fit = arch_model(returns_1h, vol='Garch', p=p, q=q, rescale=False).fit(disp='off', last_obs=None)
                        self.garch_models[symbol]["1h"] = model_fit
                        self.logger.info(f"ResearchLab: GARCH model for {symbol} 1h re-trained/fitted for features.")

                    # Predict volatility using the fitted model
                    forecast = self.garch_models[symbol]["1h"].forecast(horizon=1, start=len(returns_1h)-1)
                    predicted_vol = to_float(forecast.variance.iloc[-1].values[0])
                    
                    # Map this hourly predicted volatility to 1-minute timeframe (forward fill)
                    temp_vol_series = pd.Series(predicted_vol, index=[df_1h_for_garch.index[-1]])
                    features_df["Volatility_GARCH_Predicted_1h"] = temp_vol_series.reindex(features_df.index, method='ffill')
                    features_df["Volatility_GARCH_Predicted_1h"].fillna(0, inplace=True)

                except Exception as e:
                    self.logger.warning(f"ResearchLab: Error calculating/predicting GARCH volatility for {symbol} 1h: {e}")
                    features_df["Volatility_GARCH_Predicted_1h"] = 0.0
            else: features_df["Volatility_GARCH_Predicted_1h"] = 0.0
        else: features_df["Volatility_GARCH_Predicted_1h"] = 0.0

        # Standard Indicators (RSI, MACD, Bollinger Bands)
        if not ohlcv_df_dict["1m"].empty:
            df_1m_full_for_indicators = ohlcv_df_dict["1m"].copy()
            close_1m = df_1m_full_for_indicators['close'].values
            high_1m = df_1m_full_for_indicators['high'].values
            low_1m = df_1m_full_for_indicators['low'].values

            if len(close_1m) >= 30: # Min data for these indicators
                features_df["RSI_14_last"] = pd.Series(talib.RSI(close_1m, timeperiod=14), index=df_1m_full_for_indicators.index)
                macd, macdsignal, macdhist = talib.MACD(close_1m, fastperiod=12, slowperiod=26, signalperiod=9)
                features_df["MACD_Hist_last"] = pd.Series(macdhist, index=df_1m_full_for_indicators.index)
                upper_bb, mid_bb, lower_bb = talib.BBANDS(close_1m, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
                features_df["BB_Width_1m_Pct"] = pd.Series((upper_bb - lower_bb) / mid_bb * 100, index=df_1m_full_for_indicators.index)
            
            features_df.fillna(0, inplace=True) # Fill NaNs that arise from indicator lookbacks

        # Candlestick Patterns (example: Doji)
        if not ohlcv_df_dict["1m"].empty and len(ohlcv_df_dict["1m"]) >= 5:
            df_1m_patterns = ohlcv_df_dict["1m"].copy()
            open_p, high_p, low_p, close_p = df_1m_patterns['open'].values, df_1m_patterns['high'].values, df_1m_patterns['low'].values, df_1m_patterns['close'].values
            doji = talib.CDLDOJI(open_p, high_p, low_p, close_p)
            features_df["Price_Action_Pattern_Doji"] = pd.Series(np.where(doji != 0, 1, 0), index=df_1m_patterns.index)
            features_df["Price_Action_Pattern_Doji"].fillna(0, inplace=True) # Fill starting NaNs

        # Orderbook Imbalance (conceptual for backtest, needs proper orderbook reconstruction)
        # This feature is typically derived from level 2 data (depth updates). For backtest,
        # if only ticks are available, this would be an approximation or need deep historical order book data.
        # For simplicity, we create a placeholder based on some volume imbalance or OFI.
        features_df["Orderbook_Imbalance_Top10_Ratio"] = features_df['OFI_10m_candle'] # Placeholder for OB imbalance
        
        # Ensure all selected features are in the dataframe, fill missing with 0 if calculation failed
        for f_name in self.config["research"]["ml_features_to_use"]:
            if f_name not in features_df.columns:
                features_df[f_name] = 0.0

        features_df = features_df.reindex(ohlcv_df_dict["1m"].index, method='ffill').bfill() # Final forward/backward fill to match 1m index
        features_df.dropna(inplace=True) # Remove any remaining NaNs (should be minimal)

        self.logger.info(f"ResearchLab: Historical features calculated. Shape: {features_df.shape}.")
        return features_df

    def _generate_labels(self, ohlcv_1m_df: pd.DataFrame, future_target_minutes: int, price_change_threshold_pct: float) -> pd.Series:
        """Generates labels (target) for ML training based on future price movement."""
        self.logger.info("ResearchLab: Generating labels for ML training...")
        
        target_offset_candles = future_target_minutes * 60 // timeframe_to_seconds("1m")
        if len(ohlcv_1m_df) <= target_offset_candles:
            self.logger.warning("ResearchLab: Not enough data to generate labels with target offset. Skipping.")
            return pd.Series(index=ohlcv_1m_df.index, dtype=int)
        
        # Calculate future price (shifted close)
        future_price = ohlcv_1m_df['close'].shift(-target_offset_candles)
        
        # Calculate future price change percentage
        price_change_pct = (future_price - ohlcv_1m_df['close']) / ohlcv_1m_df['close'] * 100
        
        # Define labels: 1 for UP, 0 for DOWN (binary classification)
        labels = pd.Series(np.nan, index=ohlcv_1m_df.index, dtype=int)
        labels[price_change_pct > price_change_threshold_pct] = 1 # UP
        labels[price_change_pct < -price_change_threshold_pct] = 0 # DOWN
        
        self.logger.info(f"ResearchLab: Labels generated: {labels.value_counts()} (UP: 1, DOWN: 0).")
        return labels.dropna()

    async def train_new_model(self, symbol: str, start_date: str, end_date: str, model_version: str = "v1"):
        """Trains a new ML Alpha model."""
        self.logger.info(f"--- ResearchLab: STARTING MODEL TRAINING '{model_version}' for {symbol} ---")
        
        # 1. Load raw tick data
        tick_df = await asyncio.to_thread(self._load_raw_tick_data, symbol, start_date, end_date)
        if tick_df.empty:
            self.logger.error("ResearchLab: No tick data for training. Training cancelled.")
            return

        # 2. Reconstruct OHLCV for all timeframes needed for features
        ohlcv_dict = {
            tf_str: self._reconstruct_ohlcv_from_ticks(tick_df, timeframe_to_seconds(tf_str))
            for tf_str in CONFIG["exchange"]["default_timeframes_for_ohlcv_reconstruction"]
        }
        
        # 3. Calculate all features
        features_df = self._calculate_all_features_for_df(ohlcv_dict, tick_df, symbol)
        
        # 4. Generate labels
        labels = self._generate_labels(
            ohlcv_dict["1m"],
            CONFIG["research"]["train_model_future_price_target_minutes"],
            CONFIG["research"]["train_model_price_change_threshold_pct"]
        )
        
        # 5. Merge features and labels
        data_merged = features_df.join(labels.rename('label')).dropna()
        
        if data_merged.empty:
            self.logger.error("ResearchLab: Merged data is empty after join and NaN removal. Training cancelled.")
            return
        
        X = data_merged[CONFIG["research"]["ml_features_to_use"]]
        y = data_merged['label']

        if X.empty or y.empty:
            self.logger.error("ResearchLab: X or Y is empty after data preparation. Training cancelled.")
            return
        
        if len(y.unique()) < 2:
            self.logger.warning("ResearchLab: Only one class in labels (y). Cannot train a classifier. Training cancelled.")
            return

        # 6. Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        self.feature_scaler = scaler 
        
        joblib.dump(scaler, CONFIG["research"]["feature_scaler_save_path"])
        self.logger.info(f"ResearchLab: Feature scaler saved to {CONFIG['research']['feature_scaler_save_path']}.")

        # 7. Split data into training and validation sets
        X_train, X_val, y_train, y_val = train_test_split(X_scaled, y, test_size=0.2, random_state=42, stratify=y)
        
        self.logger.info(f"ResearchLab: Training set size: {X_train.shape}")
        self.logger.info(f"ResearchLab: Validation set size: {X_val.shape}")

        # 8. Train the LightGBM model
        self.logger.info("ResearchLab: Training LightGBM model...")
        model = lgb.LGBMClassifier(objective='binary', metric='auc', n_estimators=1000, learning_rate=0.05, num_leaves=31, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], callbacks=[lgb.early_stopping(100, verbose=False)])
        
        self.logger.info("ResearchLab: LightGBM model trained.")

        # 9. Evaluate the model
        y_pred_proba = model.predict_proba(X_val)[:, 1] 
        roc_auc = roc_auc_score(y_val, y_pred_proba)
        
        y_pred = (y_pred_proba > 0.5).astype(int) 
        report = classification_report(y_val, y_pred, output_dict=True)
        
        self.logger.info("--- ResearchLab: Model Evaluation Report ---")
        self.logger.info(f"AUC ROC: {roc_auc:.4f}")

        # --- CORRECTION APPLIQUÉE ICI ---
        # On vérifie si les clés sont '1' et '0' (pour les labels entiers) 
        # ou '1.0' et '0.0' (pour les labels flottants), ce qui évite l'erreur.
        up_label = '1' if '1' in report else '1.0'
        down_label = '0' if '0' in report else '0.0'

        if up_label in report and down_label in report:
            self.logger.info(f"Precision (Class UP): {report[up_label]['precision']:.4f}")
            self.logger.info(f"Recall (Class UP): {report[up_label]['recall']:.4f}")
            self.logger.info(f"F1-Score (Class UP): {report[up_label]['f1-score']:.4f}")
            self.logger.info(f"Precision (Class DOWN): {report[down_label]['precision']:.4f}")
            self.logger.info(f"Recall (Class DOWN): {report[down_label]['recall']:.4f}")
            self.logger.info(f"F1-Score (Class DOWN): {report[down_label]['f1-score']:.4f}")
        else:
            self.logger.warning("Impossible d'afficher le rapport détaillé par classe. Contenu du rapport :")
            self.logger.warning(report)
            
        self.logger.info("----------------------------------")

        # Check performance threshold for saving
        performance_metric_value = 0.0
        if self.config["research"]["model_validation_metric"] == "roc_auc":
            performance_metric_value = roc_auc
        elif self.config["research"]["model_validation_metric"] == "accuracy":
            performance_metric_value = report['accuracy']
        elif self.config["research"]["model_validation_metric"] == "f1":
            performance_metric_value = report['weighted avg']['f1-score']
        
        if performance_metric_value >= self.config["research"]["model_min_performance_threshold"]:
            # 10. Save the trained model
            model_filename = os.path.join(self.model_save_dir, f"{CONFIG['research']['ml_model_save_prefix']}_{model_version}.joblib")
            joblib.dump(model, model_filename)
            self.logger.info(f"ResearchLab: Trained model saved to {model_filename} (Performance: {performance_metric_value:.4f}).")
            
            # Save the GARCH models (if any were re-trained/fitted)
            joblib.dump(self.garch_models, CONFIG["research"]["garch_model_save_path"])
            self.logger.info(f"ResearchLab: GARCH models saved to {CONFIG['research']['garch_model_save_path']}.")
        else:
            self.logger.warning(f"ResearchLab: Model performance ({performance_metric_value:.4f}) below threshold ({self.config['research']['model_min_performance_threshold']:.4f}). Model not saved.")

        self.logger.info(f"--- ResearchLab: MODEL TRAINING '{model_version}' FINISHED ---")

    async def run_backtest(self, symbol: str, start_date: str, end_date: str):
        """Launches an event-driven backtest simulation."""
        self.logger.info(f"\n--- ResearchLab: STARTING BACKTEST for {symbol} ({start_date} to {end_date}) ---")

        # --- CORRECTION APPLIQUÉE ICI ---
        # On crée une nouvelle file d'événements vide juste pour ce backtest.
        backtest_event_queue = asyncio.Queue()

        # Initialize a dedicated backtest controller instance
        backtest_bot_controller = BacktestBotController(
            backtest_event_queue, CONFIG, self # On utilise la nouvelle file d'événements
        )
        
        # Run the backtest sequence within the controller
        await backtest_bot_controller.run(symbol, start_date, end_date)
        
        # After backtest, retrieve final state and metrics
        final_backtest_state = backtest_bot_controller.get_backtest_results()
        metrics = self._calculate_backtest_metrics(final_backtest_state)

        self.logger.info("\n--- ResearchLab: FINAL BACKTEST SUMMARY ---")
        for k, v in metrics.items(): self.logger.info(f"  {k}: {v}")
        self.logger.info("------------------------------------------")

        # Optional: Save backtest results and detailed trade log
        self._save_backtest_results(final_backtest_state, metrics, symbol, start_date, end_date)

        self.logger.info(f"--- ResearchLab: BACKTEST for {symbol} FINISHED ---")
        return metrics

    async def walk_forward_optimization(self, symbol: str, total_start_date: str, total_end_date: str):
        """Performs a walk-forward optimization of the ML model and strategy parameters."""
        self.logger.info(f"\n--- ResearchLab: STARTING WALK-FORWARD OPTIMIZATION for {symbol} ---")
        
        train_window_days = self.config["research"]["walk_forward_validation_window_days"]
        test_window_days = self.config["research"]["walk_forward_test_window_days"]
        
        current_train_start_dt = datetime.datetime.strptime(total_start_date, "%Y-%m-%d")
        total_end_dt = datetime.datetime.strptime(total_end_date, "%Y-%m-%d")
        
        overall_walk_forward_results = []
        iteration = 0

        while True:
            iteration += 1
            train_end_dt = current_train_start_dt + datetime.timedelta(days=train_window_days)
            test_start_dt = train_end_dt + datetime.timedelta(days=1)
            test_end_dt = test_start_dt + datetime.timedelta(days=test_window_days)

            # Ensure test window does not exceed overall end date
            if test_start_dt > total_end_dt:
                self.logger.info("ResearchLab: Remaining data is insufficient for a full test window. Walk-forward ending.")
                break
            if test_end_dt > total_end_dt:
                test_end_dt = total_end_dt # Adjust test end to not go beyond total_end_date
                self.logger.warning(f"ResearchLab: Adjusted final test window end to {test_end_dt.strftime('%Y-%m-%d')}.")
            
            if current_train_start_dt >= train_end_dt: # Avoid empty training window
                self.logger.warning("ResearchLab: Training window start is not before end. Adjusting walk-forward steps or data range.")
                break # Should adjust params to avoid this

            self.logger.info(f"\n--- ResearchLab: WALK-FORWARD ITERATION {iteration} ---")
            self.logger.info(f"  Training Period: {current_train_start_dt.strftime('%Y-%m-%d')} to {train_end_dt.strftime('%Y-%m-%d')}")
            self.logger.info(f"  Test Period:     {test_start_dt.strftime('%Y-%m-%d')} to {test_end_dt.strftime('%Y-%m-%d')}")

            # Step 1: Train/Retrain the ML model on the current training window
            # This will save a new model version (e.g., v_WF_20230101) and update the scaler
            await self.train_new_model(
                symbol, 
                current_train_start_dt.strftime('%Y-%m-%d'), 
                train_end_dt.strftime('%Y-%m-%d'), 
                model_version=f"WF_{current_train_start_dt.strftime('%Y%m%d')}"
            )

            # Step 2: Run backtest on the out-of-sample test window with the newly trained model
            # The BacktestBotController will load the latest saved model/scaler automatically
            backtest_metrics = await self.run_backtest(
                symbol, 
                test_start_dt.strftime('%Y-%m-%d'), 
                test_end_dt.strftime('%Y-%m-%d')
            )
            
            if backtest_metrics:
                iteration_result = {
                    "iteration": iteration,
                    "train_period": f"{current_train_start_dt.strftime('%Y-%m-%d')} to {train_end_dt.strftime('%Y-%m-%d')}",
                    "test_period": f"{test_start_dt.strftime('%Y-%m-%d')} to {test_end_dt.strftime('%Y-%m-%d')}",
                    "metrics": backtest_metrics,
                }
                overall_walk_forward_results.append(iteration_result)
                LOGGER.info(f"ResearchLab: Walk-Forward Iteration {iteration} completed. Test metrics: {backtest_metrics}")
            else:
                LOGGER.error(f"ResearchLab: Walk-Forward Iteration {iteration} backtest failed. Skipping results for this iteration.")

            # Move to the next training window (shift by test_window_days)
            current_train_start_dt = current_train_start_dt + datetime.timedelta(days=test_window_days)
        
        self.logger.info("\n--- ResearchLab: WALK-FORWARD OPTIMIZATION COMPLETED ---")
        
        # Summarize overall Walk-Forward performance
        if overall_walk_forward_results:
            total_returns = [res['metrics']['Total Return %'] for res in overall_walk_forward_results if 'Total Return %' in res['metrics']]
            avg_sharpe = np.mean([res['metrics']['Sharpe Ratio'] for res in overall_walk_forward_results if 'Sharpe Ratio' in res['metrics']])
            avg_drawdown = np.mean([res['metrics']['Max Drawdown %'] for res in overall_walk_forward_results if 'Max Drawdown %' in res['metrics']])
            
            self.logger.info(f"ResearchLab: Overall Walk-Forward Results ({len(overall_walk_forward_results)} iterations):")
            self.logger.info(f"  Average Test Return: {np.mean(total_returns):.2f}%")
            self.logger.info(f"  Average Test Sharpe Ratio: {avg_sharpe:.3f}")
            self.logger.info(f"  Average Test Max Drawdown: {avg_drawdown:.2f}%")

            # Save full walk-forward results
            results_filename = os.path.join(self.model_save_dir, f"walk_forward_results_{symbol.replace('/','')}_{total_start_date}_{total_end_date}.json")
            with open(results_filename, 'w') as f:
                json.dump(overall_walk_forward_results, f, indent=4)
            self.logger.info(f"ResearchLab: Full walk-forward results saved to {results_filename}.")
        else:
            self.logger.warning("ResearchLab: No successful walk-forward iterations completed.")

        return overall_walk_forward_results

    def _calculate_backtest_metrics(self, backtest_state: Dict) -> Dict:
        """Calculates and returns performance metrics for a completed backtest."""
        equity_curve = backtest_state["equity_curve"]
        trade_history = backtest_state["trade_history"]

        if equity_curve.empty:
            self.logger.warning("ResearchLab: Equity curve is empty. No backtest metrics.")
            return {}
        
        final_equity = equity_curve.iloc[-1]
        initial_capital = backtest_state["initial_capital"]
        total_net_profit = final_equity - initial_capital
        total_return_pct = (total_net_profit / initial_capital) * 100 if initial_capital > 0 else 0.0

        num_days = (equity_curve.index[-1] - equity_curve.index[0]).days
        if num_days == 0 and len(equity_curve) > 1: num_days = 1 # Handle very short backtests
        
        returns = equity_curve.pct_change().dropna()
        sharpe_ratio = calculate_sharpe_ratio(returns, periods_per_year=252 if num_days > 0 else 1) # Use 252 for daily, 1 for less than a year
        
        max_drawdown_pct, _ = calculate_drawdown(equity_curve)

        num_trades = len(trade_history)
        if num_trades > 0:
            winning_trades = [t for t in trade_history if t.get('pnl_usd', 0) > 0]
            losing_trades = [t for t in trade_history if t.get('pnl_usd', 0) < 0]
            num_wins = len(winning_trades)
            num_losses = len(losing_trades)
            win_rate = (num_wins / num_trades) * 100
            gross_profit = sum(t['pnl_usd'] for t in winning_trades)
            gross_loss = abs(sum(t['pnl_usd'] for t in losing_trades))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
            
            avg_pnl_per_trade = total_net_profit / num_trades
            avg_win_pnl = gross_profit / num_wins if num_wins > 0 else 0.0
            avg_loss_pnl = gross_loss / num_losses if num_losses > 0 else 0.0
            avg_win_loss_ratio = abs(avg_win_pnl / avg_loss_pnl) if avg_loss_pnl > 0 else float('inf')
            total_fees = sum(t.get('total_fees', 0.0) for t in trade_history)
        else:
            win_rate = 0.0
            profit_factor = 0.0
            avg_pnl_per_trade = 0.0
            avg_win_pnl = 0.0
            avg_loss_pnl = 0.0
            avg_win_loss_ratio = 0.0
            total_fees = 0.0

        return {
            "Initial Capital": round(initial_capital, 2),
            "Final Equity": round(final_equity, 2),
            "Total Net Profit": round(total_net_profit, 2),
            "Total Return %": round(total_return_pct, 2),
            "Duration (Days)": num_days,
            "Sharpe Ratio": round(sharpe_ratio, 3),
            "Max Drawdown %": round(max_drawdown_pct, 2),
            "Total Trades": num_trades,
            "Winning Trades": num_wins if num_trades > 0 else 0,
            "Losing Trades": num_losses if num_trades > 0 else 0,
            "Win Rate %": round(win_rate, 2),
            "Profit Factor": round(profit_factor, 3),
            "Avg Trade PNL": round(avg_pnl_per_trade, 3),
            "Avg Win PNL": round(avg_win_pnl, 3),
            "Avg Loss PNL": round(avg_loss_pnl, 3),
            "Avg Win/Loss Ratio": round(avg_win_loss_ratio, 2),
            "Total Fees Paid": round(total_fees, 2),
        }

    def _save_backtest_results(self, backtest_state: Dict, metrics: Dict, symbol: str, start_date: str, end_date: str):
        """Saves backtest results and trade history to JSON/CSV files."""
        report_dir = os.path.join(self.model_save_dir, "backtest_reports")
        os.makedirs(report_dir, exist_ok=True)
        
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_prefix = f"{symbol.replace('/','')}__{start_date}_{end_date}__{timestamp_str}"

        # Save metrics
        metrics_filepath = os.path.join(report_dir, f"metrics_{filename_prefix}.json")
        with open(metrics_filepath, 'w') as f:
            json.dump(metrics, f, indent=4)
        self.logger.info(f"ResearchLab: Backtest metrics saved to {metrics_filepath}.")

        # Save trade history
        trade_history_filepath = os.path.join(report_dir, f"trades_{filename_prefix}.json")
        # Need to serialize datetime objects in trade_history
        serializable_trade_history = []
        for trade in backtest_state["trade_history"]:
            trade_copy = trade.copy()
            for key in ["entry_time", "close_time", "last_update_timestamp"]:
                if key in trade_copy and isinstance(trade_copy[key], (float, int)):
                    trade_copy[key] = convert_ms_to_datetime_utc(int(trade_copy[key] * 1000)).isoformat() # Assume float seconds -> ms -> iso
            serializable_trade_history.append(trade_copy)

        with open(trade_history_filepath, 'w') as f:
            json.dump(serializable_trade_history, f, indent=4)
        self.logger.info(f"ResearchLab: Backtest trade history saved to {trade_history_filepath}.")

        # Save equity curve (CSV for easy plotting)
        equity_curve_filepath = os.path.join(report_dir, f"equity_{filename_prefix}.csv")
        backtest_state["equity_curve"].to_csv(equity_curve_filepath, header=True)
        self.logger.info(f"ResearchLab: Backtest equity curve saved to {equity_curve_filepath}.")

# ==============================================================================
# SECTION 4 (SUITE): SIMULATED BOT CONTROLLER POUR LE BACKTEST
# ==============================================================================
# Cette classe simule l'environnement du BotController en mode backtest,
# utilisant des versions simulées des modules pour rejouer les données historiques.

class BacktestBotController:
    def __init__(self, event_queue: asyncio.Queue, config: Dict, research_lab_instance: 'ResearchLab'):
        self.event_queue = event_queue
        self.config = config
        self.logger = LOGGER
        self.research_lab = research_lab_instance # Allows access to historical data loading and feature calculation
        
        # Internal state for backtesting simulation
        self.backtest_state = {
            "cash": self.config["research"]["backtest_initial_capital"],
            "equity": self.config["research"]["backtest_initial_capital"],
            "initial_capital": self.config["research"]["backtest_initial_capital"],
            "positions": {}, # {pos_id: position_dict}
            "open_orders": {}, # {order_id: order_dict} (simulated open orders)
            "trade_history": [],
            "equity_curve": pd.Series(dtype=float),
            "current_sim_time_ms": 0, # Current timestamp in simulation (in ms)
            "sim_ohlcv_data": defaultdict(lambda: defaultdict(pd.DataFrame)), # {symbol: {timeframe: full_historical_df}}
            "sim_features_data": defaultdict(pd.DataFrame), # {symbol: full_historical_features_df}
            "sim_order_book_bids_asks": defaultdict(lambda: {'bids': {}, 'asks': {}, 'timestamp': 0}), # Simulated orderbook for fills
        }
        self.backtest_shared_lock = asyncio.Lock() # Lock for backtest_state

        # Simulated modules instances (these interact with backtest_state directly)
        self.modules: Dict[str, Any] = {
            "DataHandler": self._SimulatedDataHandler(self.event_queue, self.config, self.research_lab, self.backtest_state, self.backtest_shared_lock),
            "AlphaModel": AlphaModel(self.event_queue, self.config),
            "PortfolioRiskManager": self._SimulatedPortfolioRiskManager(self.event_queue, self.config, self.backtest_state, self.backtest_shared_lock),
            "ExecutionHandler": self._SimulatedExecutionHandler(self.event_queue, self.config, self.backtest_state, self.backtest_shared_lock),
            "UIManager": UIManager(self.event_queue, self.config)
        }
        # Force UIManager to log to console only for backtests or use a dedicated backtest logger
        self.modules["UIManager"]._running = True # Make it active for backtest alerts
        self.modules["UIManager"].alert_config["enable_alerts"] = True # Ensure alerts are enabled for backtest

        self.logger.info("BacktestBotController initialized for simulation.")

    async def run(self, symbol: str, start_date: str, end_date: str):
        """Main backtesting loop: loads data, simulates events, and collects results."""
        self.logger.info(f"BacktestBotController: Starting simulation for {symbol} from {start_date} to {end_date}.")
        
        # Load all historical data and pre-calculate all features for backtest
        await self.modules["DataHandler"]._load_and_process_historical_data(symbol, start_date, end_date)
        
        # Get the 1-minute OHLCV data to drive the simulation loop
        ohlcv_1m_data_for_loop = self.modules["DataHandler"].get_simulated_ohlcv_data(symbol, "1m")
        if ohlcv_1m_data_for_loop.empty:
            self.logger.error("BacktestBotController: No 1-minute OHLCV data to drive simulation. Aborting.")
            return

        # Start event dispatch loop (simulated)
        event_dispatch_task = asyncio.create_task(self._backtest_event_dispatch_loop(), name="BacktestEventDispatchLoop")

        # Start simulated module run loops
        sim_module_tasks = []
        for name, module in self.modules.items():
            if name not in ["DataHandler", "UIManager"]: # DataHandler is managed by main backtest loop, UIManager is passive
                if hasattr(module, 'run'):
                    sim_module_tasks.append(asyncio.create_task(module.run(), name=f"BacktestModuleTask-{name}"))

        self.logger.info(f"BacktestBotController: Starting main simulation loop over {len(ohlcv_1m_data_for_loop)} 1-minute candles.")
        
        # Main simulation loop: Iterate over 1-minute candles
        for idx, current_1m_candle in ohlcv_1m_data_for_loop.iterrows():
            async with self.backtest_shared_lock:
                self.backtest_state["current_sim_time_ms"] = current_1m_candle.name.timestamp() * 1000
                
                # Update current OHLCV and Features for modules to consume
                for tf_str, df_tf in self.modules["DataHandler"].simulated_ohlcv_data[symbol].items():
                    if idx in df_tf.index: # Ensure candle exists for this timeframe
                        self.backtest_state["sim_ohlcv_data"][symbol][tf_str] = df_tf.loc[idx]
                
                if idx in self.modules["DataHandler"].simulated_features_data[symbol].index:
                    self.backtest_state["sim_features_data"][symbol] = self.modules["DataHandler"].simulated_features_data[symbol].loc[idx]

                # Update equity curve at the beginning of each candle based on last candle's close
                await self._update_equity_curve_in_backtest(idx, current_1m_candle['close'])

            # Trigger feature event
            features_at_time = self.backtest_state["sim_features_data"][symbol]
            if not features_at_time.empty:
                await self.event_queue.put(FeatureEvent(symbol, features_at_time.to_dict(), self.backtest_state["current_sim_time_ms"]))
            
            # Simulate order fills and position management (handled by ExecutionHandler & PRM triggered by time)
            await self.modules["ExecutionHandler"]._backtest_pass_time(self.backtest_state["current_sim_time_ms"], symbol, current_1m_candle)
            await self.modules["PortfolioRiskManager"]._backtest_update_portfolio_metrics(symbol, self.backtest_state["current_sim_time_ms"])

            # Add a small yield to allow other coroutines to run (e.g., event dispatcher)
            await asyncio.sleep(0) # Yield control

        self.logger.info("BacktestBotController: Main simulation loop finished.")

        # Final equity update at the very end
        if not ohlcv_1m_data_for_loop.empty:
            final_candle = ohlcv_1m_data_for_loop.iloc[-1]
            await self._update_equity_curve_in_backtest(final_candle.name, final_candle['close'])
        
        # Stop all simulated module tasks
        for task in sim_module_tasks: task.cancel()
        event_dispatch_task.cancel()
        try: # Wait for them to actually cancel
            await asyncio.gather(event_dispatch_task, *sim_module_tasks, return_exceptions=True)
        except asyncio.CancelledError: pass # Expected

        self.logger.info("BacktestBotController: Simulation finished. Collecting results.")

    async def _backtest_event_dispatch_loop(self):
        """Simulated event dispatch loop for backtest."""
        while True:
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=0.01) # Small timeout
                
                if event.type == "FEATURE_UPDATE":
                    await self.modules["AlphaModel"].on_feature_event(event)
                elif event.type == "SIGNAL":
                    await self.modules["PortfolioRiskManager"].on_signal_event(event)
                elif event.type == "ORDER_REQUEST":
                    await self.modules["ExecutionHandler"].on_order_request_event(event)
                elif event.type == "ORDER_UPDATE":
                    await self.modules["PortfolioRiskManager"].on_order_update_event(event)
                elif event.type == "POSITION_UPDATE":
                    await self.modules["UIManager"].on_position_update_event(event)
                elif event.type == "SYSTEM_ALERT":
                    await self.modules["UIManager"].on_system_alert_event(event) # UIManager just logs
                
                self.event_queue.task_done()
            except asyncio.TimeoutError:
                pass # No events, continue
            except asyncio.CancelledError:
                LOGGER.debug("Backtest event dispatch loop cancelled.")
                break
            except Exception as e:
                LOGGER.error(f"Backtest dispatch error: {e}", exc_info=True)
                break

    async def _update_equity_curve_in_backtest(self, timestamp: pd.Timestamp, current_price: float):
        """Updates the backtest equity curve based on current simulated state."""
        async with self.backtest_shared_lock:
            # Calculate current total equity
            cash = self.backtest_state["cash"]
            total_unrealized_pnl = 0.0
            total_margin_used = 0.0

            for pos_id, pos in self.backtest_state["positions"].items():
                if pos["status"] == "open":
                    pos_pnl = calculate_pnl_usd(pos["entry_price"], current_price, pos["current_amount"], pos["side"])
                    total_unrealized_pnl += pos_pnl
                    total_margin_used += pos.get("margin_used", 0.0) # Assume margin_used is updated

            # Equity = Cash + (Margin of Open Positions) + Unrealized PnL
            current_equity = cash + total_margin_used + total_unrealized_pnl
            self.backtest_state["equity"] = current_equity
            self.backtest_state["equity_curve"].at[timestamp] = current_equity

    def get_backtest_results(self) -> Dict:
        """Returns the final state of the backtest."""
        return self.backtest_state

    # --- Simulated Modules for Backtesting ---

    class _SimulatedDataHandler:
        """
        Un gestionnaire de données simulé pour le mode backtest.
        Il pré-charge toutes les données historiques et les fonctionnalités, puis les
        sert au fur et à mesure que la simulation avance dans le temps.
        """
        def __init__(self, event_queue: asyncio.Queue, config: Dict, research_lab_instance: 'ResearchLab', backtest_state: Dict, backtest_shared_lock: asyncio.Lock):
            # L'appel à super().__init__(...) a été supprimé car il n'y a pas de classe parente.
            # Nous initialisons les attributs nécessaires directement ici.
            self.event_queue = event_queue
            self.config = config
            self.logger = LOGGER
            self.ohlcv_timeframes = {tf: timeframe_to_seconds(tf) for tf in config["exchange"]["default_timeframes_for_ohlcv_reconstruction"]}
            
            # Le reste de l'initialisation
            self.research_lab = research_lab_instance
            self.backtest_state = backtest_state
            self.backtest_shared_lock = backtest_shared_lock
            self.simulated_ohlcv_data = defaultdict(lambda: defaultdict(pd.DataFrame))
            self.simulated_features_data = defaultdict(pd.DataFrame)
            self._running = True # Toujours actif en mode backtest pour servir les données

        async def _load_and_process_historical_data(self, symbol: str, start_date: str, end_date: str):
            self.logger.info(f"SimDataHandler: Loading and preprocessing historical data for {symbol}...")
            
            # Load raw tick data
            tick_df = await asyncio.to_thread(self.research_lab._load_raw_tick_data, symbol, start_date, end_date)
            if tick_df.empty:
                raise ValueError("SimDataHandler: No tick data loaded for backtest.")
            
            # Reconstruct OHLCV for all required timeframes
            for tf_str, tf_sec in self.ohlcv_timeframes.items():
                if tf_sec > 0:
                    ohlcv_df = self.research_lab._reconstruct_ohlcv_from_ticks(tick_df, tf_sec)
                    if not ohlcv_df.empty:
                        ohlcv_df.name = symbol # For GARCH
                        self.simulated_ohlcv_data[symbol][tf_str] = ohlcv_df
                        self.logger.info(f"SimDataHandler: OHLCV {tf_str} reconstructed: {len(ohlcv_df)} candles.")

            # Calculate all features on the historical data
            # This is done once for the entire backtest, then features are served based on time
            if "1m" in self.simulated_ohlcv_data[symbol] and not self.simulated_ohlcv_data[symbol]["1m"].empty:
                self.simulated_features_data[symbol] = self.research_lab._calculate_all_features_for_df(
                    self.simulated_ohlcv_data[symbol], tick_df, symbol
                )
                self.logger.info(f"SimDataHandler: {len(self.simulated_features_data[symbol])} sets of features calculated.")
            else:
                raise ValueError("SimDataHandler: OHLCV 1m data is required for feature calculation.")

        def get_simulated_ohlcv_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
            return self.simulated_ohlcv_data[symbol][timeframe]
        
        def get_simulated_features(self, symbol: str, timestamp: pd.Timestamp) -> Optional[pd.Series]:
            if symbol in self.simulated_features_data and not self.simulated_features_data[symbol].empty:
                try:
                    return self.simulated_features_data[symbol].loc[timestamp]
                except KeyError:
                    # If exact timestamp not found, get the closest one
                    closest_features = self.simulated_features_data[symbol].loc[self.simulated_features_data[symbol].index <= timestamp].iloc[-1]
                    return closest_features
            return None
        
        async def run(self):
            # Simulated DataHandler does not run its own continuous loop.
            # Its data is pre-loaded and served by the BacktestBotController's main loop.
            LOGGER.info("Simulated DataHandler running in passive mode (serving pre-loaded data).")
            # Keep alive until cancelled
            try: await asyncio.Future()
            except asyncio.CancelledError: pass

        async def _connect_websocket(self, uri: str): pass
        async def _process_ws_message(self, message: str): pass
        async def _process_trade_tick(self, symbol: str, data: Dict): pass
        async def _process_depth_update(self, symbol: str, data: Dict): pass
        async def _feature_calculation_loop(self, symbol: str): pass

        async def stop(self):
            self._running = False
            LOGGER.info("Simulated DataHandler stopped.")


    class _SimulatedPortfolioRiskManager(PortfolioRiskManager):
        def __init__(self, event_queue: asyncio.Queue, config: Dict, backtest_state: Dict, backtest_shared_lock: asyncio.Lock):
            super().__init__(event_queue, config)
            self.backtest_state = backtest_state
            self.backtest_shared_lock = backtest_shared_lock
            self.exchange_client = None # No real exchange client in backtest

        async def run(self):
            LOGGER.info("Simulated PortfolioRiskManager running in backtest mode.")
            # Keep alive until cancelled
            try: await asyncio.Future()
            except asyncio.CancelledError: pass

        async def _update_capital_and_positions_from_exchange(self):
            # In backtest, capital and positions are updated internally via fill events, not API calls
            pass

        async def _backtest_update_portfolio_metrics(self, symbol: str, current_time_ms: int):
            """Updates PnL, leverage, and manages SL/TP/TSL for backtest."""
            async with self.backtest_shared_lock:
                # Update current_utc_ms for consistent timestamps
                SHARED_STATE["current_utc_ms"] = current_time_ms

                positions_to_manage = list(self.backtest_state["positions"].items())

                for pos_id, position in positions_to_manage:
                    if position["status"] != "open": continue

                    current_price = self._get_current_price_from_backtest_state(symbol)
                    if current_price is None: continue # Cannot manage without price

                    # Update unrealized PnL in position state
                    pos_pnl_usd = calculate_pnl_usd(position['entry_price'], current_price, position['current_amount'], position['side'])
                    position['pnl_usd'] = pos_pnl_usd
                    if position.get('margin_used', 0) > 0:
                        position['pnl_percentage'] = (pos_pnl_usd / position['margin_used']) * 100
                    else: position['pnl_percentage'] = 0.0

                    # Update highest/lowest profit price for TSL
                    if position["side"] == "BUY":
                        position["highest_profit_price"] = max(position.get("highest_profit_price", position["entry_price"]), current_price)
                    else:
                        position["lowest_profit_price"] = min(position.get("lowest_profit_price", position["entry_price"]), current_price)
                    position["last_update_timestamp"] = current_time_ms

                    # Get historical OHLCV for SL/TP/TSL calculations (from DataHandler's loaded data)
                    ohlcv_1m_data_for_sl_tp = self.backtest_state["sim_ohlcv_data"][symbol]["1m"]
                    
                    # Call core SL/TP/TSL check logic (from parent class)
                    await self._check_trailing_stop_loss(position, current_price, ohlcv_1m_data_for_sl_tp)
                    await self._check_hard_stop_loss(position, current_price)
                    await self._check_take_profit_levels(position, current_price)

        def _get_current_price_from_backtest_state(self, symbol: str) -> Optional[float]:
            # In backtest, get price from the current simulated candle's close
            current_candle_data = self.backtest_state["sim_ohlcv_data"][symbol].get("1m")
            if current_candle_data is not None and not current_candle_data.empty:
                return current_candle_data['close']
            return None

        async def _place_initial_exit_orders(self, position: Dict):
            # In backtest, no need to actually call exchange API. Just simulate sending OrderRequestEvent.
            # The simulated ExecutionHandler will handle creating the order.
            symbol = position["symbol"]
            amount = position["initial_amount"]
            
            if position["initial_stop_loss"]:
                await self.event_queue.put(OrderRequestEvent(
                    request_type="CREATE",
                    symbol=symbol,
                    order_type="STOP_MARKET", 
                    side="SELL" if position["side"] == "BUY" else "BUY",
                    amount=amount,
                    price=None,
                    purpose="INITIAL_STOP_LOSS",
                    position_id=position["position_id"],
                    params={"stopPrice": position["initial_stop_loss"]}
                ))
            
            if position["take_profit_levels"]:
                first_tp = position["take_profit_levels"][0]
                await self.event_queue.put(OrderRequestEvent(
                    request_type="CREATE",
                    order_type="LIMIT",
                    symbol=symbol,
                    side="SELL" if position["side"] == "BUY" else "BUY",
                    amount=amount * (first_tp["amount_pct_original"] / 100.0), # Proportion of initial amount
                    price=first_tp["price"],
                    purpose=f"TAKE_PROFIT_L{first_tp['level']}",
                    position_id=position["position_id"]
                ))
        
        async def _move_stop_loss(self, position: Dict, new_sl_price: float, reason: str):
            # Simulate modification
            await self.event_queue.put(OrderRequestEvent(
                request_type="MODIFY",
                symbol=position["symbol"],
                order_type="STOP_MARKET",
                side="SELL" if position["side"] == "BUY" else "BUY",
                amount=position["current_amount"],
                price=None,
                purpose=reason,
                position_id=position["position_id"],
                original_order_id=position["stop_loss_order_id"],
                params={"stopPrice": new_sl_price}
            ))

        async def _place_take_profit_order(self, position: Dict, level: int, price: float, amount: float):
            # Simulate placing TP order
            await self.event_queue.put(OrderRequestEvent(
                request_type="CREATE",
                order_type="LIMIT",
                symbol=position["symbol"],
                side="SELL" if position["side"] == "BUY" else "BUY",
                amount=amount,
                price=price,
                purpose=f"TAKE_PROFIT_L{level}",
                position_id=position["position_id"]
            ))

        async def _cancel_and_replace_exit_orders(self, position: Dict):
            # Simulate cancelling and replacing
            await self.event_queue.put(OrderRequestEvent(
                request_type="CANCEL_ALL_FOR_POS",
                symbol=position["symbol"],
                position_id=position["position_id"],
                purpose="CANCEL_ON_SCALE"
            ))
            # Re-issue initial exit orders based on new avg entry/size (logic from on_signal_event)
            # This needs to be carefully handled to avoid infinite loops if it calls on_signal_event.
            # Best to have a specific _recalculate_and_place_exits method here.
            # For backtest, PRM would recalculate and send events to EH.
            await self.on_signal_event(SignalEvent(
                symbol=position["symbol"], side=position["side"], confidence=position["confidence_at_entry"],
                strategy_id=position["strategy_id"], trigger_price=position["entry_price"], # New averaged entry price
                features_at_signal=position["features_at_entry"], signal_timestamp_ms=get_current_utc_ms()
            ))

        async def _close_position_finalize(self, position: Dict, final_fill_price: float, reason: str):
            async with self.backtest_shared_lock:
                # Remove from backtest state, add to history
                self.backtest_state["positions"].pop(position["position_id"])
                position["status"] = "closed"
                position["close_time"] = self.backtest_state["current_sim_time_ms"] / 1000
                position["close_price"] = final_fill_price
                position["duration_seconds"] = position["close_time"] - (position["entry_time"] if isinstance(position["entry_time"], (float,int)) else position["entry_time"].timestamp())
                
                # PnL already accumulated in 'realized_pnl'
                pnl_usd = position["realized_pnl"]
                pnl_pct = (pnl_usd / (position["entry_price"] * position["original_amount"])) * 100 if position["entry_price"] * position["original_amount"] > 1e-9 else 0.0
                
                position["pnl_usd"] = pnl_usd
                position["pnl_percentage"] = pnl_pct
                position["close_reason"] = reason

                self.backtest_state["trade_history"].append(copy.deepcopy(position))
                LOGGER.info(f"BACKTEST: POSITION FINALIZED: {position['symbol']} {position['side']}. Reason: {reason}. PnL: {pnl_usd:.4f} ({pnl_pct:.2f}%)")
                # No actual alerts, just log
                await self.event_queue.put(SystemAlertEvent("INFO", f"BACKTEST_ALERT: CLOSED: {position['symbol']} {position['side']}. PnL: {pnl_usd:.2f} ({pnl_pct:.2f}%)."))


    class _SimulatedExecutionHandler(ExecutionHandler):
        def __init__(self, event_queue: asyncio.Queue, config: Dict, backtest_state: Dict, backtest_shared_lock: asyncio.Lock):
            super().__init__(event_queue, config)
            self.backtest_state = backtest_state
            self.backtest_shared_lock = backtest_shared_lock
            self.exchange_client = None # No real exchange client in backtest

        async def run(self):
            LOGGER.info("Simulated ExecutionHandler running in backtest mode (passive).")
            try: await asyncio.Future()
            except asyncio.CancelledError: pass

        async def _create_order_with_retry(self, symbol: str, order_type: str, side: str, amount: float, price: Optional[float], params: Dict) -> Optional[Dict]:
            """Simulates order creation and returns an order object."""
            order_id = generate_unique_id("sim_order")
            simulated_order = {
                "id": order_id, "symbol": symbol, "type": order_type, "side": side, "amount": amount, "price": price,
                "clientOrderId": params.get("clientOrderId"), "info": params, "status": "new", "filled": 0.0,
                "remaining": amount, "average": price, "fee": {"cost": 0.0, "currency": CONFIG["trading"]["capital_currency"]},
                "timestamp": int(self.backtest_state["current_sim_time_ms"]) # Order creation time in simulation
            }

            async with self.backtest_shared_lock:
                self.backtest_state["open_orders"][order_id] = simulated_order
                self.backtest_state["telemetry_data"]["orders_placed_sim"] = self.backtest_state["telemetry_data"].get("orders_placed_sim", 0) + 1
            LOGGER.debug(f"BACKTEST: Simulating order creation: {order_id} ({order_type} {side} {symbol}).")
            return simulated_order

        async def _emit_order_update_event(self, order: Dict, purpose: str, pos_id: str):
            # In backtest, we directly emit the update after internal processing.
            # This is called by _simulate_fill_order and other methods here.
            status = order.get('status', 'unknown').lower()
            fill_event = OrderUpdateEvent(
                exchange_order_id=order["id"], client_order_id=order.get("clientOrderId"), symbol=order["symbol"],
                status=status, filled_amount=to_float(order.get('filled')), remaining_amount=to_float(order.get('remaining')),
                fill_price=to_float(order.get('average', order.get('price'))), purpose=purpose, position_id=pos_id,
                fee=to_float(order.get('fee', {}).get('cost')), fee_currency=order.get('fee', {}).get('currency'),
                order_details=order
            )
            await self.event_queue.put(fill_event)

        async def _backtest_pass_time(self, current_sim_time_ms: int, symbol: str, current_1m_candle: pd.Series):
            """Simulates order fills based on current candle's price range."""
            async with self.backtest_shared_lock:
                open_orders_copy = list(self.backtest_state["open_orders"].values())
            
            for order in open_orders_copy:
                if order["status"] not in ["new", "open", "partial"]: continue # Only check active orders
                
                # Check for fill conditions based on current candle's OHLCV
                # Use current_1m_candle directly (passed from backtest main loop)
                high = current_1m_candle['high']
                low = current_1m_candle['low']
                open_px = current_1m_candle['open']
                close_px = current_1m_candle['close']
                
                fill_price = None
                filled_qty = 0.0

                # Determine potential fill price based on order type and price action
                if order["type"].upper() == "MARKET":
                    fill_price = open_px # Market orders typically fill at the open of the next candle
                    filled_qty = order["amount"]
                elif "stopPrice" in order["info"]: # Stop Orders (Stop Market)
                    stop_price = to_float(order["info"]["stopPrice"])
                    triggered = False
                    if order["side"].upper() == "SELL" and low <= stop_price: # Sell Stop (Long SL)
                        triggered = True
                        fill_price = max(stop_price, open_px) # Fill at stop price or open if gapped
                    elif order["side"].upper() == "BUY" and high >= stop_price: # Buy Stop (Short SL)
                        triggered = True
                        fill_price = min(stop_price, open_px)
                    if triggered: filled_qty = order["amount"]
                elif order["type"].upper() == "LIMIT": # Limit Orders
                    limit_price = to_float(order["price"])
                    triggered = False
                    if order["side"].upper() == "BUY" and low <= limit_price: # Buy Limit
                        triggered = True
                        fill_price = limit_price # Fill at limit price
                    elif order["side"].upper() == "SELL" and high >= limit_price: # Sell Limit
                        triggered = True
                        fill_price = limit_price
                    if triggered: filled_qty = order["amount"]
                
                if filled_qty > 0 and fill_price is not None:
                    await self._simulate_fill_order(order, filled_qty, fill_price)

        async def _simulate_fill_order(self, order: Dict, filled_amount: float, fill_price: float):
            """Simulates the fill process for an order."""
            fees_pct = self.config["research"]["backtest_fees_pct"]
            slippage_pct_factor = self.config["research"]["backtest_slippage_pct"]
            
            # Apply slippage
            final_fill_price = fill_price
            if order["type"].upper() == "MARKET" or "stopPrice" in order["info"]: # Market or Stop Market orders incur slippage
                if order["side"].upper() == "BUY":
                    final_fill_price = fill_price * (1 + slippage_pct_factor)
                else: # SELL
                    final_fill_price = fill_price * (1 - slippage_pct_factor)
            
            # Adjust to simulated exchange precision (using current_sim_time_ms is conceptual here)
            # In a real backtest, this uses the historical market info for precision.
            final_fill_price = adjust_price_for_order(order["symbol"], final_fill_price, None) # Use general price adjust

            order_value = filled_amount * final_fill_price
            fee_cost = order_value * fees_pct
            
            order["status"] = "filled"
            order["filled"] = filled_amount
            order["remaining"] = 0.0
            order["average"] = final_fill_price
            order["fee"]["cost"] = fee_cost
            order["timestamp"] = int(self.backtest_state["current_sim_time_ms"]) # Fill time in simulation

            # Update backtest cash
            async with self.backtest_shared_lock:
                if order["side"].upper() == "BUY":
                    self.backtest_state["cash"] -= (order_value + fee_cost)
                else: # SELL
                    self.backtest_state["cash"] += (order_value - fee_cost)
                self.backtest_state["open_orders"].pop(order["id"], None)
                
                self.backtest_state["telemetry_data"]["orders_filled_sim"] = self.backtest_state["telemetry_data"].get("orders_filled_sim", 0) + 1
                self.backtest_state["telemetry_data"]["total_fees_sim"] = self.backtest_state["telemetry_data"].get("total_fees_sim", 0.0) + fee_cost

            # Emit OrderUpdateEvent to notify PRM for position updates
            await self._emit_order_update_event(order, order["info"].get("purpose", "SIMULATED_FILL"), order["info"].get("position_id", "UNKNOWN"))
            LOGGER.debug(f"BACKTEST: Order {order['id']} ({order['symbol']}) FILLED at {final_fill_price:.5f} (Qty: {filled_amount:.8f}).")

        async def _reconcile_open_orders(self): pass # No actual reconciliation in backtest
        async def _cancel_order(self, order_request: Dict):
            # Simulate cancel
            async with self.backtest_shared_lock:
                order_id_to_cancel = order_request["original_order_id"]
                order = self.backtest_state["open_orders"].get(order_id_to_cancel)
                if order:
                    order["status"] = "canceled"
                    order["remaining"] = order["amount"] - order["filled"]
                    self.backtest_state["open_orders"].pop(order_id_to_cancel)
                    LOGGER.debug(f"BACKTEST: Order {order_id_to_cancel} ({order['symbol']}) simulated CANCELLED.")
                    await self._emit_order_update_event(order, order_request["purpose"], order["info"].get("position_id", "N/A"))
        async def _modify_order(self, order_request: Dict):
            # Simulate modify as cancel+create
            await self._cancel_order(order_request) # Simulate cancel
            await self._create_order_with_retry(
                order_request["symbol"], order_request["order_type"], order_request["side"],
                order_request["amount"], order_request["price"], order_request["params"]
            )
        async def _cancel_all_orders_for_position(self, pos_id: str, symbol: str):
            async with self.backtest_shared_lock:
                orders_to_cancel_ids = [oid for oid, odata in self.backtest_state["open_orders"].items() if odata.get("info",{}).get("position_id") == pos_id]
            for oid in orders_to_cancel_ids:
                await self._cancel_order({"original_order_id": oid, "symbol": symbol})

        async def _cancel_all_open_orders_on_exchange(self):
            async with self.backtest_shared_lock:
                orders_to_cancel_ids = list(self.backtest_state["open_orders"].keys())
            for oid in orders_to_cancel_ids:
                await self._cancel_order({"original_order_id": oid, "symbol": self.backtest_state["open_orders"][oid]["symbol"]})
            LOGGER.critical("BACKTEST: All open orders simulated cancelled.")

        async def _emergency_close_position(self, order_request: Dict):
            # In backtest, it's a market fill at current price
            current_price = self.backtest_state["sim_ohlcv_data"][order_request["symbol"]]["1m"]['close']
            await self._simulate_fill_order(
                {"id": generate_unique_id("sim_emergency"), "symbol": order_request["symbol"], "type": "market", "side": order_request["side"], "amount": order_request["amount"]},
                order_request["amount"], current_price, is_market_order=True
            )

# ==============================================================================
# SECTION 6: POINT D'ENTRÉE PRINCIPAL
# ==============================================================================

async def main_async():
    parser = argparse.ArgumentParser(
        description="KRONOS Monolith - Advanced Crypto Trading Bot Framework",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--mode', type=str, choices=['live', 'backtest', 'train', 'walk_forward'], default='live',
        help="Operating mode: 'live', 'backtest', 'train', 'walk_forward'."
    )
    parser.add_argument('--symbol', type=str, default=CONFIG["exchange"]["default_symbol"], help="Symbol for operations (e.g., 'BTC/USDT').")
    parser.add_argument('--start-date', type=str, help="Start date (YYYY-MM-DD) for backtest/train/walk_forward.")
    parser.add_argument('--end-date', type=str, help="End date (YYYY-MM-DD) for backtest/train/walk_forward.")
    parser.add_argument('--model-version', type=str, default="v1", help="Version of the model to train/test.")
    
    args = parser.parse_args()

    # Initialize ResearchLab as it's needed for train/backtest/walk_forward modes
    research_lab = ResearchLab(CONFIG)

    if args.mode == 'train':
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required for 'train' mode.")
        LOGGER.info(f"Launching bot in model training mode for {args.symbol}...")
        await research_lab.train_new_model(args.symbol, args.start_date, args.end_date, args.model_version)
        LOGGER.info("Model training finished.")
    elif args.mode == 'backtest':
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required for 'backtest' mode.")
        LOGGER.info(f"Launching bot in backtest mode for {args.symbol}...")
        await research_lab.run_backtest(args.symbol, args.start_date, args.end_date)
        LOGGER.info("Backtest finished.")
    elif args.mode == 'walk_forward':
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required for 'walk_forward' mode.")
        LOGGER.info(f"Launching bot in walk-forward optimization mode for {args.symbol}...")
        await research_lab.walk_forward_optimization(args.symbol, args.start_date, args.end_date)
        LOGGER.info("Walk-forward optimization finished.")
    elif args.mode == 'live':
        LOGGER.info("Launching bot in LIVE TRADING (or Paper Trading if sandbox enabled) mode...")
        
        global_bot_controller_instance = BotController(mode="live")
        
        # Setting up signal handlers for graceful shutdown (Ctrl+C, kill)
        import signal # <--- AJOUTEZ CET IMPORT

        loop = asyncio.get_running_loop()
        def handle_signal(sig_name):
            LOGGER.critical(f"Signal {sig_name} received. Initiating bot shutdown procedure.")
            asyncio.create_task(global_bot_controller_instance.shutdown())

        # Tente d'ajouter les gestionnaires de signaux, mais ne plante pas si ce n'est pas supporté (utile pour Windows)
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: handle_signal(s.name))
        except (NotImplementedError, AttributeError) as e:
            LOGGER.warning(f"Could not set up signal handlers: {e}. Graceful shutdown via Ctrl+C might not work. Please stop the bot manually if needed.")

        try:
            await global_bot_controller_instance.run()
        except asyncio.CancelledError:
            LOGGER.info("Main bot task was cancelled.")
        except Exception as e:
            LOGGER.critical(f"Unhandled error in main bot loop: {e}", exc_info=True)
            asyncio.create_task(global_bot_controller_instance.shutdown()) # Attempt graceful shutdown

    LOGGER.info("Main program finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user (Ctrl+C).")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nExecution finished. KRONOS says goodbye.")