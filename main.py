import ccxt
import pandas as pd
import numpy as np
import ta
from datetime import datetime, timedelta
import time
import logging
import json
import os
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# Настройка логирования с временем
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_futures_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FuturesCryptoTradingBot:
    def __init__(self):
        # Настройки биржи фьючерсов Binance
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # Для фьючерсов
                'adjustForTimeDifference': True
            }
        })
        
        # Множественные таймфреймы
        self.timeframes = ['5m', '15m', '1h', '4h']
        
        # Список фьючерсных пар (30+ токенов)
        self.symbols = [
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT',
            'ADA/USDT', 'DOGE/USDT', 'DOT/USDT', 'AVAX/USDT', 'LINK/USDT',
            'UNI/USDT', 'LTC/USDT', 'ATOM/USDT', 'ETC/USDT',
            'FIL/USDT', 'TRX/USDT', 'VET/USDT', 'XLM/USDT', 'ICP/USDT',
            'FTM/USDT', 'HBAR/USDT', 'NEAR/USDT', 'ALGO/USDT', 'EGLD/USDT',
            'FLOW/USDT', 'SAND/USDT', 'MANA/USDT', 'AXS/USDT', 'GALA/USDT',
            'APE/USDT', 'CHZ/USDT', 'ENJ/USDT', 'THETA/USDT', 'GMT/USDT'
        ]
        
        # Хранилища данных
        self.active_trades = {}
        self.signal_history = defaultdict(list)
        self.signals_found = []
        self.analysis_stats = {
            'total_analyzed': 0,
            'signals_generated': 0,
            'start_time': datetime.now().isoformat()
        }
        
        # Имя файла для сохранения состояния
        self.state_file = 'bot_state.json'
        
        # Технические параметры для краткосрочной торговли
        self.risk_params = {
            'min_confidence_threshold': 50,
            'min_volume_filter': 500000,
            'min_rr_ratio': 1.3,  # Уменьшен для большей гибкости
            'use_short_signals': True
        }
        
        # Загрузка состояния при инициализации
        self.load_state()
        
        # Инициализация
        self.load_market_data()
        
    def load_state(self):
        """Загрузка состояния бота из файла"""
        try:
            if os.path.exists(self.state_file):
                if os.path.getsize(self.state_file) == 0:
                    logger.warning(f"⚠️  Файл состояния {self.state_file} пустой, создаю новый")
                    self.create_default_state_file()
                    return
                    
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:
                        logger.warning(f"⚠️  Файл состояния {self.state_file} пустой, создаю новый")
                        self.create_default_state_file()
                        return
                        
                    state = json.loads(content)
                    
                if 'active_trades' in state:
                    self.active_trades = self.convert_to_serializable(state['active_trades'])
                    logger.info(f"📥 Восстановлено {len(self.active_trades)} активных сделок из состояния")
                    
                if 'signal_history' in state:
                    raw_history = state['signal_history']
                    self.signal_history = defaultdict(list)
                    for symbol, signals in raw_history.items():
                        self.signal_history[symbol] = [self.convert_to_serializable(signal) for signal in signals]
                    total_signals = sum(len(signals) for signals in self.signal_history.values())
                    logger.info(f"📥 Восстановлено {total_signals} сигналов из истории")
                    
                if 'signals_found' in state:
                    self.signals_found = [self.convert_to_serializable(signal) for signal in state['signals_found']]
                    logger.info(f"📥 Восстановлено {len(self.signals_found)} найденных сигналов")
                    
                if 'analysis_stats' in state:
                    self.analysis_stats = self.convert_to_serializable(state['analysis_stats'])
                    logger.info("📥 Статистика восстановлена")
                    
                logger.info("✅ Состояние бота успешно загружено")
            else:
                logger.info("🆕 Новый запуск бота - файл состояния не найден")
                self.create_default_state_file()
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка JSON в файле состояния: {e}")
            logger.warning("🔄 Создаю новый файл состояния...")
            self.create_default_state_file()
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояния: {e}")
            logger.warning("🔄 Создаю новый файл состояния...")
            self.create_default_state_file()
            
    def create_default_state_file(self):
        """Создание файла состояния по умолчанию"""
        try:
            default_state = {
                'active_trades': {},
                'signal_history': {},
                'signals_found': [],
                'analysis_stats': {
                    'total_analyzed': 0,
                    'signals_generated': 0,
                    'start_time': datetime.now().isoformat()
                },
                'created_at': datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(default_state, f, ensure_ascii=False, indent=2, default=str)
                
            logger.info("✅ Создан новый файл состояния по умолчанию")
        except Exception as e:
            logger.error(f"❌ Ошибка создания файла состояния: {e}")
            
    def convert_to_serializable(self, obj):
        """Конвертация объекта в сериализуемый формат"""
        if isinstance(obj, dict):
            return {key: self.convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_serializable(item) for item in obj]
        elif isinstance(obj, (np.integer, np.floating)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif pd.api.types.is_integer_dtype(type(obj)) or pd.api.types.is_float_dtype(type(obj)):
            return float(obj)
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
            
    def save_state(self):
        """Сохранение текущего состояния бота"""
        try:
            state = {
                'active_trades': self.convert_to_serializable(self.active_trades),
                'signal_history': self.convert_to_serializable(dict(self.signal_history)),
                'signals_found': self.convert_to_serializable(self.signals_found),
                'analysis_stats': self.convert_to_serializable(self.analysis_stats),
                'saved_at': datetime.now().isoformat()
            }
            
            temp_file = self.state_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2, default=str)
            
            os.replace(temp_file, self.state_file)
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")
            
    def load_market_data(self):
        """Загрузка данных о фьючерсных рынках"""
        try:
            markets = self.exchange.load_markets()
            futures_symbols = [symbol for symbol in self.symbols if symbol in markets]
            self.symbols = futures_symbols
            logger.info(f"Загружено {len(markets)} фьючерсных рынков")
            logger.info(f"Активные фьючерсные пары: {len(self.symbols)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки фьючерсных рынков: {e}")
            
    def fetch_ohlcv_multitimeframe(self, symbol):
        """Получение фьючерсных данных по нескольким таймфреймам"""
        data = {}
        try:
            for tf in self.timeframes:
                limit = 200 if tf in ['5m', '15m'] else 100
                ohlcv = self.exchange.fetch_ohlcv(symbol, tf, limit=limit)
                if len(ohlcv) > 0:
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    data[tf] = df
                else:
                    data[tf] = None
        except Exception as e:
            logger.error(f"Ошибка получения фьючерсных данных для {symbol}: {e}")
            return None
        return data
        
    def calculate_advanced_indicators(self, df, timeframe):
        """Расчет расширенных технических индикаторов"""
        if df is None or len(df) < 20:
            return None
            
        try:
            # EMA
            df['ema_9'] = ta.trend.EMAIndicator(df['close'], window=9).ema_indicator()
            df['ema_21'] = ta.trend.EMAIndicator(df['close'], window=21).ema_indicator()
            df['ema_50'] = ta.trend.EMAIndicator(df['close'], window=50).ema_indicator()
            
            # MACD
            df['macd'] = df['ema_9'] - df['ema_21']
            df['macd_signal'] = df['macd'].rolling(window=9).mean()
            df['macd_histogram'] = df['macd'] - df['macd_signal']
            
            # RSI
            df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
            df['rsi_sma'] = df['rsi'].rolling(window=14).mean()
            
            # Стохастик
            stoch = ta.momentum.StochasticOscillator(df['high'], df['low'], df['close'], window=14, smooth_window=3)
            df['stoch_k'] = stoch.stoch()
            df['stoch_d'] = stoch.stoch_signal()
            
            # Боллинджер
            bb = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
            df['bb_upper'] = bb.bollinger_hband()
            df['bb_middle'] = bb.bollinger_mavg()
            df['bb_lower'] = bb.bollinger_lband()
            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / (df['bb_middle'] + 0.0001)
            df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'] + 0.0001)
            
            # ATR
            df['atr'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range()
            
            # Объем
            df['volume_ema'] = ta.trend.EMAIndicator(df['volume'], window=20).ema_indicator()
            df['volume_ratio'] = (df['volume'] / (df['volume_ema'] + 0.0001))
            
            # Моментум
            df['roc_1'] = df['close'].pct_change(1)
            df['roc_3'] = df['close'].pct_change(3)
            df['roc_7'] = df['close'].pct_change(7)
            
            # Паттерны
            df['candle_body'] = abs(df['close'] - df['open'])
            df['candle_ratio'] = df['candle_body'] / (df['high'] - df['low'] + 0.0001)
            
            # Тренд
            df['price_trend_20'] = (df['close'] - df['close'].shift(20)) / (df['close'].shift(20) + 0.0001)
            df['price_trend_50'] = (df['close'] - df['close'].shift(50)) / (df['close'].shift(50) + 0.0001)
            
            # Свинг хай/лоу для уровней поддержки/сопротивления
            df['swing_high'] = df['high'][
                (df['high'] > df['high'].shift(1)) & 
                (df['high'] > df['high'].shift(-1)) & 
                (df['high'] > df['high'].shift(2)) & 
                (df['high'] > df['high'].shift(-2))
            ]
            
            df['swing_low'] = df['low'][
                (df['low'] < df['low'].shift(1)) & 
                (df['low'] < df['low'].shift(-1)) & 
                (df['low'] < df['low'].shift(2)) & 
                (df['low'] < df['low'].shift(-2))
            ]
            
            # Пивотные точки
            df['pivot'] = (df['high'] + df['low'] + df['close']) / 3
            df['pivot_r1'] = 2 * df['pivot'] - df['low']
            df['pivot_s1'] = 2 * df['pivot'] - df['high']
            
        except Exception as e:
            logger.error(f"Ошибка расчета индикаторов: {e}")
            return df
            
        return df
        
    def calculate_multitimeframe_analysis(self, data_dict):
        """Мультивременной анализ"""
        if not data_dict:
            return {}
            
        analysis = {}
        
        for tf, df in data_dict.items():
            if df is not None and len(df) > 0:
                try:
                    last_row = df.iloc[-1]
                    analysis[tf] = {
                        'price': float(last_row['close']) if not pd.isna(last_row['close']) else 0,
                        'trend_ema': 'bullish' if last_row['close'] > last_row.get('ema_21', last_row['close']) else 'bearish',
                        'rsi': float(last_row.get('rsi', 50)) if not pd.isna(last_row.get('rsi', 50)) else 50,
                        'macd_trend': 'bullish' if (last_row.get('macd', 0) or 0) > (last_row.get('macd_signal', 0) or 0) else 'bearish',
                        'momentum': float(last_row.get('roc_3', 0)) if not pd.isna(last_row.get('roc_3', 0)) else 0,
                        'bb_position': float(last_row.get('bb_position', 0.5)) if not pd.isna(last_row.get('bb_position', 0.5)) else 0.5
                    }
                except Exception as e:
                    logger.debug(f"Ошибка анализа таймфрейма {tf}: {e}")
                    analysis[tf] = {
                        'price': float(df['close'].iloc[-1]) if len(df) > 0 and not pd.isna(df['close'].iloc[-1]) else 0,
                        'trend_ema': 'neutral',
                        'rsi': 50,
                        'macd_trend': 'neutral',
                        'momentum': 0,
                        'bb_position': 0.5
                    }
        
        return analysis
        
    def calculate_dynamic_levels(self, symbol, data_dict, signal_type):
        """Расчет динамических TP и SL на основе потенциала роста и индикаторов"""
        if not data_dict or '1h' not in data_dict:
            return self.calculate_basic_levels(symbol, data_dict, signal_type)
            
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 20:
            return self.calculate_basic_levels(symbol, data_dict, signal_type)
            
        try:
            current_price = float(df_1h['close'].iloc[-1])
            atr = float(df_1h['atr'].iloc[-1]) if 'atr' in df_1h.columns and not pd.isna(df_1h['atr'].iloc[-1]) else current_price * 0.02
            
            # НОРМАЛЬНЫЕ базовые уровни
            base_sl_distance = atr * 1.3
            base_tp1_distance = atr * 2.0
            base_tp2_distance = atr * 3.2
            base_tp3_distance = atr * 4.8
            
            try:
                # НОРМАЛЬНЫЙ анализ потенциала
                rsi = float(df_1h['rsi'].iloc[-1]) if not pd.isna(df_1h['rsi'].iloc[-1]) else 50
                bb_position = float(df_1h['bb_position'].iloc[-1]) if not pd.isna(df_1h['bb_position'].iloc[-1]) else 0.5
                momentum_1h = float(df_1h['roc_7'].iloc[-1]) if not pd.isna(df_1h['roc_7'].iloc[-1]) else 0
                volume_ratio = float(df_1h['volume_ratio'].iloc[-1]) if not pd.isna(df_1h['volume_ratio'].iloc[-1]) else 1
                
                # Анализ тренда
                trend_strength_4h = 0
                trend_strength_1h = 0
                
                if '4h' in data_dict and data_dict['4h'] is not None and len(data_dict['4h']) > 20:
                    df_4h = data_dict['4h']
                    trend_4h = (df_4h['close'].iloc[-1] - df_4h['close'].iloc[-20]) / df_4h['close'].iloc[-20]
                    trend_strength_4h = float(trend_4h) if not pd.isna(trend_4h) else 0
                    
                trend_strength_1h = float(df_1h['price_trend_20'].iloc[-1]) if not pd.isna(df_1h['price_trend_20'].iloc[-1]) else 0
                
                # НОРМАЛЬНЫЙ множитель потенциала (0.8 - 1.5)
                potential_multiplier = 1.0
                
                if signal_type == 'LONG':
                    rsi_factor = max(0.8, min(1.4, (55 - rsi) / 30))  # 0.8 - 1.4
                    bb_factor = max(0.8, min(1.4, (1.0 - bb_position) * 1.8))  # 0.8 - 1.4
                    momentum_factor = max(0.85, min(1.3, 1.0 + momentum_1h * 8))  # 0.85 - 1.3
                    trend_factor = max(0.85, min(1.3, 1.0 + (trend_strength_4h + trend_strength_1h) * 4))  # 0.85 - 1.3
                    volume_factor = max(0.9, min(1.2, volume_ratio * 0.4 + 0.8))  # 0.9 - 1.2
                    
                    potential_multiplier = (rsi_factor + bb_factor + momentum_factor + trend_factor + volume_factor) / 5
                    potential_multiplier = max(0.85, min(1.4, potential_multiplier))  # 0.85 - 1.4
                    
                else:  # SHORT
                    rsi_factor = max(0.8, min(1.4, (rsi - 45) / 30))  # 0.8 - 1.4
                    bb_factor = max(0.8, min(1.4, bb_position * 1.8))  # 0.8 - 1.4
                    momentum_factor = max(0.85, min(1.3, 1.0 - momentum_1h * 8))  # 0.85 - 1.3
                    trend_factor = max(0.85, min(1.3, 1.0 - (trend_strength_4h + trend_strength_1h) * 4))  # 0.85 - 1.3
                    volume_factor = max(0.9, min(1.2, volume_ratio * 0.4 + 0.8))  # 0.9 - 1.2
                    
                    potential_multiplier = (rsi_factor + bb_factor + momentum_factor + trend_factor + volume_factor) / 5
                    potential_multiplier = max(0.85, min(1.4, potential_multiplier))  # 0.85 - 1.4
                
                # Расчет уровней
                if signal_type == 'LONG':
                    sl = current_price - (base_sl_distance * 0.9)  # SL немного меньше
                    tp1 = current_price + (base_tp1_distance * potential_multiplier)
                    tp2 = current_price + (base_tp2_distance * potential_multiplier * 1.05)
                    tp3 = current_price + (base_tp3_distance * potential_multiplier * 1.1)
                    
                else:  # SHORT
                    sl = current_price + (base_sl_distance * 0.9)  # SL немного меньше
                    tp1 = current_price - (base_tp1_distance * potential_multiplier)
                    tp2 = current_price - (base_tp2_distance * potential_multiplier * 1.05)
                    tp3 = current_price - (base_tp3_distance * potential_multiplier * 1.1)
                
                # Проверка валидности
                rr_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
                if rr_ratio < 0.8 or rr_ratio > 8:  # Разумный диапазон
                    raise Exception("Неразумный RR")
                    
                if signal_type == 'LONG':
                    if sl >= current_price or tp3 <= current_price:
                        raise Exception("Неразумные уровни LONG")
                else:
                    if sl <= current_price or tp3 >= current_price:
                        raise Exception("Неразумные уровни SHORT")
                
                return round(float(sl), 8), round(float(tp1), 8), round(float(tp2), 8), round(float(tp3), 8)
                
            except Exception as e:
                logger.debug(f"⚠️  {symbol}: Использую базовые уровни из-за: {e}")
                return self.calculate_basic_levels(symbol, data_dict, signal_type)
                
        except Exception as e:
            logger.error(f"❌ {symbol}: Критическая ошибка, использую базовые уровни: {e}")
            return self.calculate_basic_levels(symbol, data_dict, signal_type)

    def calculate_basic_levels(self, symbol, data_dict, signal_type):
        """Базовые уровни на случай ошибок"""
        try:
            if not data_dict or '1h' not in data_dict or data_dict['1h'] is None:
                # Абсолютно базовые значения
                current_price = 1000.0  # Значение по умолчанию для теста
            else:
                current_price = float(data_dict['1h']['close'].iloc[-1])
                
            atr = current_price * 0.015  # 1.5% от цены
            
            if signal_type == 'LONG':
                sl = current_price - atr * 1.2
                tp1 = current_price + atr * 1.8
                tp2 = current_price + atr * 3.0
                tp3 = current_price + atr * 4.5
            else:
                sl = current_price + atr * 1.2
                tp1 = current_price - atr * 1.8
                tp2 = current_price - atr * 3.0
                tp3 = current_price - atr * 4.5
                
            logger.debug(f"🔄 {symbol} {signal_type} | БАЗОВЫЕ уровни: "
                        f"SL: {sl:.8f} | TP1: {tp1:.8f} | TP2: {tp2:.8f} | TP3: {tp3:.8f}")
            
            return round(float(sl), 8), round(float(tp1), 8), round(float(tp2), 8), round(float(tp3), 8)
            
        except Exception as e:
            logger.error(f"❌ {symbol}: Ошибка базовых уровней: {e}")
            # Абсолютный fallback
            current_price = 1000.0
            if signal_type == 'LONG':
                return 990.0, 1005.0, 1015.0, 1025.0
            else:
                return 1010.0, 995.0, 985.0, 975.0
    
    def find_resistance_levels(self, df, current_price):
        """Поиск уровней сопротивления"""
        if df is None or len(df) < 20:
            return []
            
        try:
            # Свинг хай
            swing_highs = df['swing_high'].dropna().tail(10)
            resistance_from_swing = list(swing_highs.values) if len(swing_highs) > 0 else []
            
            # Пивотные уровни выше текущей цены
            pivot_resistance = []
            if 'pivot_r1' in df.columns:
                recent_pivots = df['pivot_r1'].tail(5).dropna()
                pivot_resistance = [p for p in recent_pivots if p > current_price]
            
            # Уровни выше текущей цены за последние 50 баров
            recent_highs = df['high'].tail(50)
            dynamic_resistance = [h for h in recent_highs if h > current_price * 1.01]  # На 1% выше
            
            # Объединяем все уровни
            all_resistance = resistance_from_swing + pivot_resistance + dynamic_resistance
            return sorted(list(set(all_resistance))) if all_resistance else []
            
        except Exception as e:
            logger.error(f"Ошибка поиска уровней сопротивления: {e}")
            return []
    
    def find_support_levels(self, df, current_price):
        """Поиск уровней поддержки"""
        if df is None or len(df) < 20:
            return []
            
        try:
            # Свинг лоу
            swing_lows = df['swing_low'].dropna().tail(10)
            support_from_swing = list(swing_lows.values) if len(swing_lows) > 0 else []
            
            # Пивотные уровни ниже текущей цены
            pivot_support = []
            if 'pivot_s1' in df.columns:
                recent_pivots = df['pivot_s1'].tail(5).dropna()
                pivot_support = [p for p in recent_pivots if p < current_price]
            
            # Уровни ниже текущей цены за последние 50 баров
            recent_lows = df['low'].tail(50)
            dynamic_support = [l for l in recent_lows if l < current_price * 0.99]  # На 1% ниже
            
            # Объединяем все уровни
            all_support = support_from_swing + pivot_support + dynamic_support
            return sorted(list(set(all_support)), reverse=True) if all_support else []
            
        except Exception as e:
            logger.error(f"Ошибка поиска уровней поддержки: {e}")
            return []
            
    def estimate_signal_formation_time(self, data_dict):
        """Расчет примерного времени формирования сигнала"""
        if not data_dict or '1h' not in data_dict:
            return "Не определено"
            
        try:
            df_1h = data_dict['1h']
            if df_1h is None or len(df_1h) < 10:
                return "Не определено"
                
            recent_data = df_1h.tail(10)
            conditions_met_count = 0
            
            for i in range(len(recent_data)-1, -1, -1):
                row = recent_data.iloc[i]
                rsi = float(row.get('rsi', 50)) if not pd.isna(row.get('rsi', 50)) else 50
                stoch_k = float(row.get('stoch_k', 50)) if not pd.isna(row.get('stoch_k', 50)) else 50
                stoch_d = float(row.get('stoch_d', 50)) if not pd.isna(row.get('stoch_d', 50)) else 50
                macd = float(row.get('macd', 0)) if not pd.isna(row.get('macd', 0)) else 0
                macd_signal = float(row.get('macd_signal', 0)) if not pd.isna(row.get('macd_signal', 0)) else 0
                bb_position = float(row.get('bb_position', 0.5)) if not pd.isna(row.get('bb_position', 0.5)) else 0.5
                
                long_conditions = [
                    rsi < 35,
                    stoch_k < 30 and stoch_d < 30,
                    macd > macd_signal,
                    bb_position < 0.3
                ]
                
                short_conditions = [
                    rsi > 65,
                    stoch_k > 70 and stoch_d > 70,
                    macd < macd_signal,
                    bb_position > 0.7
                ]
                
                if sum(long_conditions) >= 3 or sum(short_conditions) >= 3:
                    conditions_met_count = len(recent_data) - i
                    break
                    
            if conditions_met_count > 0:
                formation_hours = conditions_met_count
                return f"~{formation_hours} часов"
            else:
                return "Условия формируются"
                
        except Exception as e:
            logger.error(f"Ошибка расчета времени формирования: {e}")
            return "Не определено"
            
    def generate_signal(self, symbol, data_dict, multitimeframe_analysis):
        """Генерация торгового сигнала с динамическими уровнями"""
        if not data_dict or '1h' not in data_dict:
            logger.debug(f"❌ {symbol}: Нет данных 1h")
            return None
            
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 20:
            logger.debug(f"❌ {symbol}: Недостаточно данных ({len(df_1h) if df_1h is not None else 0})")
            return None
            
        try:
            current_price = float(df_1h['close'].iloc[-1])
            atr = float(df_1h['atr'].iloc[-1]) if 'atr' in df_1h.columns and not pd.isna(df_1h['atr'].iloc[-1]) else current_price * 0.02
            
            # Технический анализ
            rsi = float(df_1h['rsi'].iloc[-1]) if not pd.isna(df_1h['rsi'].iloc[-1]) else 50
            stoch_k = float(df_1h['stoch_k'].iloc[-1]) if not pd.isna(df_1h['stoch_k'].iloc[-1]) else 50
            stoch_d = float(df_1h['stoch_d'].iloc[-1]) if not pd.isna(df_1h['stoch_d'].iloc[-1]) else 50
            macd = float(df_1h['macd'].iloc[-1]) if not pd.isna(df_1h['macd'].iloc[-1]) else 0
            macd_signal = float(df_1h['macd_signal'].iloc[-1]) if not pd.isna(df_1h['macd_signal'].iloc[-1]) else 0
            bb_position = float(df_1h['bb_position'].iloc[-1]) if not pd.isna(df_1h['bb_position'].iloc[-1]) else 0.5
            volume_ratio = float(df_1h['volume_ratio'].iloc[-1]) if not pd.isna(df_1h['volume_ratio'].iloc[-1]) else 1
            momentum_1h = float(df_1h['roc_3'].iloc[-1]) if not pd.isna(df_1h['roc_3'].iloc[-1]) else 0
            
            momentum_5m = 0
            momentum_15m = 0
            
            if '5m' in data_dict and data_dict['5m'] is not None and len(data_dict['5m']) > 5:
                df_5m = data_dict['5m']
                momentum_5m = float(df_5m['roc_3'].iloc[-1]) if not pd.isna(df_5m['roc_3'].iloc[-1]) else 0
                
            if '15m' in data_dict and data_dict['15m'] is not None and len(data_dict['15m']) > 5:
                df_15m = data_dict['15m']
                momentum_15m = float(df_15m['roc_3'].iloc[-1]) if not pd.isna(df_15m['roc_3'].iloc[-1]) else 0
            
            # Условия для LONG (НОРМАЛЬНЫЕ)
            long_conditions = [
                rsi < 40,  # Нормальная перепроданность
                stoch_k < 30 and stoch_d < 30,  # Нормальные уровни стохастика
                macd > macd_signal,  # MACD bullish
                bb_position < 0.3,  # Цена в нижней части Боллинджера
                volume_ratio > 1.1,  # Увеличение объема
                momentum_1h > -0.015  # Умеренный downtrend
            ]
            
            # Условия для SHORT (НОРМАЛЬНЫЕ)
            short_conditions = [
                rsi > 60,  # Нормальная перекупленность
                stoch_k > 70 and stoch_d > 70,  # Нормальные уровни стохастика
                macd < macd_signal,  # MACD bearish
                bb_position > 0.7,  # Цена в верхней части Боллинджера
                volume_ratio > 1.1,  # Увеличение объема
                momentum_1h < 0.015  # Умеренный uptrend
            ]
            
            signal_type = None
            confidence_score = 0
            
            long_score = sum(long_conditions)
            short_score = sum(short_conditions)
            
            # Сила сигнала (1-5)
            signal_strength = 0
            if long_score >= 5 or short_score >= 5:
                signal_strength = 5
            elif long_score >= 4 or short_score >= 4:
                signal_strength = 4
            elif long_score >= 3 or short_score >= 3:
                signal_strength = 3
            elif long_score >= 2 or short_score >= 2:
                signal_strength = 2
            else:
                signal_strength = 1
            
            # Проверяем, разрешены ли SHORT сигналы
            if not self.risk_params['use_short_signals']:
                short_score = 0
                short_conditions = []
            
            # НОРМАЛЬНЫЕ требования для открытия
            if long_score >= 3:  # 3 из 6
                signal_type = 'LONG'
                confidence_score = (long_score / len(long_conditions)) * 100
            elif short_score >= 3 and self.risk_params['use_short_signals']:  # 3 из 6
                signal_type = 'SHORT'
                confidence_score = (short_score / len(short_conditions)) * 100
                
            # НОРМАЛЬНЫЙ порог уверенности
            if signal_type and confidence_score >= 50:  # 50% уверенности
                # Рассчитываем динамические TP и SL
                sl, tp1, tp2, tp3 = self.calculate_dynamic_levels(symbol, data_dict, signal_type)
                
                if sl is None or tp1 is None or tp2 is None or tp3 is None:
                    logger.debug(f"❌ {symbol}: Ошибка расчета уровней")
                    return None
                
                # Проверка валидности уровней
                risk_reward_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
                
                # НОРМАЛЬНЫЙ RR ratio
                if signal_type == 'LONG' and sl < current_price and tp3 > current_price and risk_reward_ratio > 1.3:
                    valid = True
                elif signal_type == 'SHORT' and sl > current_price and tp3 < current_price and risk_reward_ratio > 1.3:
                    valid = True
                else:
                    valid = False
                    
                if valid:
                    # Расчет времени формирования сигнала
                    formation_time = self.estimate_signal_formation_time(data_dict)
                    
                    # Расчет потенциала роста для логирования
                    potential_upside = ((tp3 - current_price) / current_price * 100) if signal_type == 'LONG' else ((current_price - tp3) / current_price * 100)
                    
                    return {
                        'symbol': symbol,
                        'signal_type': signal_type,
                        'entry_price': round(float(current_price), 8),
                        'tp1': round(float(tp1), 8),
                        'tp2': round(float(tp2), 8),
                        'tp3': round(float(tp3), 8),
                        'sl': round(float(sl), 8),
                        'confidence': round(float(confidence_score), 2),
                        'risk_reward_ratio': round(float(risk_reward_ratio), 2),
                        'potential_upside': round(float(potential_upside), 2),
                        'formation_time': formation_time,
                        'signal_strength': int(signal_strength),
                        'timeframe_analysis': {
                            '5m_momentum': round(float(momentum_5m), 6),
                            '15m_momentum': round(float(momentum_15m), 6),
                            '1h_momentum': round(float(momentum_1h), 6)
                        },
                        'technical_analysis': {
                            'rsi': round(float(rsi), 2),
                            'stoch_k': round(float(stoch_k), 2),
                            'macd': round(float(macd), 8),
                            'bb_position': round(float(bb_position), 2),
                            'volume_ratio': round(float(volume_ratio), 2),
                            'potential_multiplier': round(float((abs(tp3 - current_price) / (atr * 5)) if atr > 0 else 1), 2)
                        },
                        'timestamp': datetime.now().isoformat(),
                        'conditions_met': {
                            'long_score': int(long_score),
                            'short_score': int(short_score),
                            'total_conditions': len(long_conditions) if signal_type == 'LONG' else len(short_conditions)
                        }
                    }
                    
        except Exception as e:
            logger.error(f"❌ Ошибка генерации сигнала для {symbol}: {e}")
            
        return None
        
    def get_current_price(self, symbol):
        """Получение текущей цены фьючерса"""
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker['last'])
        except Exception as e:
            logger.error(f"Ошибка получения цены фьючерса для {symbol}: {e}")
            return None
            
    def check_active_trades(self):
        """Проверка статуса всех активных сделок"""
        trades_to_remove = []
        for symbol in list(self.active_trades.keys()):
            result = self.check_trade_status(symbol)
            if result == 'closed':
                trades_to_remove.append(symbol)
                
        for symbol in trades_to_remove:
            if symbol in self.active_trades:
                del self.active_trades[symbol]
                
        if trades_to_remove or len(self.active_trades) > 0:
            self.save_state()
            
    def check_trade_status(self, symbol):
        """Проверка статуса конкретной сделки"""
        if symbol not in self.active_trades:
            return 'not_found'
            
        trade = self.active_trades[symbol]
        current_price = self.get_current_price(symbol)
        
        if current_price is None:
            logger.info(f"👀 [{symbol}] Отслеживаю {trade['signal_type']} | Цена: недоступна | Вход: {trade['entry_price']} | Статус: {self.get_trade_status(trade)}")
            return 'active'
            
        entry_price = float(trade['entry_price'])
        signal_type = trade['signal_type']
        
        logger.info(f"👀 [{symbol}] Отслеживаю {signal_type} | Цена: {current_price} | Вход: {entry_price} | Статус: {self.get_trade_status(trade)}")
        
        tp_levels = [float(trade['tp1']), float(trade['tp2']), float(trade['tp3'])]
        tp_names = ['TP1', 'TP2', 'TP3']
        
        for i, (tp, tp_name) in enumerate(zip(tp_levels, tp_names)):
            if f'{tp_name.lower()}_reached' not in trade or not trade[f'{tp_name.lower()}_reached']:
                reached = False
                if signal_type == 'LONG' and current_price >= tp:
                    reached = True
                elif signal_type == 'SHORT' and current_price <= tp:
                    reached = True
                    
                if reached:
                    trade[f'{tp_name.lower()}_reached'] = True
                    logger.info(f"🎯 [{symbol}] --- Take Profit {i+1} достигнут @ {tp} ---")
                    
                    if tp_name == 'TP3':
                        logger.info(f"🎉 [{symbol}] СДЕЛКА ЗАКРЫТА по Take Profit 3 @ {tp}")
                        self.save_state()
                        return 'closed'
                        
        sl = float(trade['sl'])
        sl_reached = False
        if signal_type == 'LONG' and current_price <= sl:
            sl_reached = True
        elif signal_type == 'SHORT' and current_price >= sl:
            sl_reached = True
            
        if sl_reached:
            trade['sl_reached'] = True
            logger.info(f"🛑 [{symbol}] СДЕЛКА ЗАКРЫТА по Stop Loss @ {sl}")
            self.save_state()
            return 'closed'
            
        return 'active'
        
    def get_trade_status(self, trade):
        """Получение статуса сделки"""
        if trade.get('tp3_reached', False):
            return 'hit TP3'
        elif trade.get('tp2_reached', False):
            return 'hit TP2'
        elif trade.get('tp1_reached', False):
            return 'hit TP1'
        elif trade.get('sl_reached', False):
            return 'hit SL'
        else:
            return 'active'
            
    def send_signal(self, signal):
        """Отправка торгового сигнала"""
        if signal is None:
            return
            
        symbol = signal['symbol']
        if symbol in self.signal_history and len(self.signal_history[symbol]) > 0:
            last_signal = self.signal_history[symbol][-1]
            try:
                last_timestamp = datetime.fromisoformat(last_signal['timestamp'].replace('Z', '+00:00')) if isinstance(last_signal['timestamp'], str) else last_signal['timestamp']
                current_timestamp = datetime.fromisoformat(signal['timestamp'].replace('Z', '+00:00')) if isinstance(signal['timestamp'], str) else signal['timestamp']
                
                if (current_timestamp - last_timestamp).total_seconds() < 3600:
                    return
            except:
                pass
                
        self.signal_history[symbol].append(self.convert_to_serializable(signal))
        if len(self.signal_history[symbol]) > 50:
            self.signal_history[symbol] = self.signal_history[symbol][-25:]
            
        signal_log_entry = {
            'timestamp': signal['timestamp'],
            'symbol': signal['symbol'],
            'signal_type': signal['signal_type'],
            'entry_price': float(signal['entry_price']),
            'tp1': float(signal['tp1']),
            'tp2': float(signal['tp2']),
            'tp3': float(signal['tp3']),
            'sl': float(signal['sl']),
            'confidence': float(signal['confidence']),
            'rr_ratio': float(signal['risk_reward_ratio']),
            'potential_upside': float(signal.get('potential_upside', 0)),
            'signal_strength': int(signal['signal_strength'])
        }
        self.signals_found.append(self.convert_to_serializable(signal_log_entry))
        
        self.analysis_stats['signals_generated'] += 1
        
        logger.info(f"✅ [{signal['symbol']}] --- ОТКРЫТА {signal['signal_type']} СДЕЛКА (Сила: {signal['signal_strength']}) @ {signal['entry_price']} ---")
        logger.info(f"   SL: {signal['sl']}, TP1: {signal['tp1']}, TP2: {signal['tp2']}, TP3: {signal['tp3']}")
        logger.info(f"   📈 Потенциал: {signal.get('potential_upside', 0):.1f}% | RR: {signal['risk_reward_ratio']}")
        
        if symbol not in self.active_trades:
            self.active_trades[signal['symbol']] = self.convert_to_serializable(signal).copy()
            for tp_name in ['tp1', 'tp2', 'tp3']:
                self.active_trades[signal['symbol']][f'{tp_name}_reached'] = False
            self.active_trades[signal['symbol']]['sl_reached'] = False
            
            self.save_state()
            
    def save_signals_log(self):
        """Сохранение лога всех найденных сигналов"""
        try:
            serializable_signals = [self.convert_to_serializable(signal) for signal in self.signals_found]
            serializable_stats = self.convert_to_serializable(self.analysis_stats)
                
            with open('signals_log.json', 'w', encoding='utf-8') as f:
                json.dump({
                    'signals': serializable_signals,
                    'stats': serializable_stats,
                    'generated_at': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2, default=str)
                
            if self.signals_found:
                csv_data = []
                for signal in self.signals_found:
                    csv_signal = self.convert_to_serializable(signal).copy()
                    csv_data.append(csv_signal)
                df = pd.DataFrame(csv_data)
                df.to_csv('signals_log.csv', index=False)
                
        except Exception as e:
            logger.error(f"Ошибка сохранения лога сигналов: {e}")
        
    def process_symbol(self, symbol):
        """Обработка одного символа"""
        try:
            data_dict = self.fetch_ohlcv_multitimeframe(symbol)
            if not data_dict:
                return None
                
            for tf in self.timeframes:
                if tf in data_dict and data_dict[tf] is not None:
                    data_dict[tf] = self.calculate_advanced_indicators(data_dict[tf], tf)
            
            multitimeframe_analysis = self.calculate_multitimeframe_analysis(data_dict)
            
            if symbol not in self.active_trades:
                signal = self.generate_signal(symbol, data_dict, multitimeframe_analysis)
                # Добавить отладку:
                if signal is None:
                    # Проверить scores для отладки
                    df_1h = data_dict.get('1h')
                    if df_1h is not None and len(df_1h) >= 20:
                        # Здесь можно добавить логирование scores
                        pass
                return signal
            else:
                logger.debug(f"⏭️  Пропущен {symbol} - уже есть активная сделка")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка обработки {symbol}: {e}")
            return None
            
    def run_analysis_cycle(self):
        """Запуск одного цикла анализа"""
        cycle_start_time = datetime.now()
        logger.info(f"🚀 Начало анализа {len(self.symbols)} фьючерсных пар...")
        
        signals = []
        processed_count = 0
        
        for symbol in self.symbols:
            try:
                signal = self.process_symbol(symbol)
                if signal:
                    signals.append(signal)
                    self.send_signal(signal)
                processed_count += 1
                self.analysis_stats['total_analyzed'] += 1
                
                if processed_count % 5 == 0:
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке {symbol}: {e}")
        
        self.check_active_trades()
        self.save_signals_log()
        
        cycle_duration = datetime.now() - cycle_start_time
        logger.info(f"✅ Цикл анализа завершен за {cycle_duration.total_seconds():.1f} секунд")
        logger.info(f"📊 Обработано {processed_count} пар. Найдено {len(signals)} сигналов.")
        
        self.save_state()
        
    def run(self):
        """Основной цикл работы бота"""
        logger.info("🚀 Запуск фьючерсного криптотрейдинг бота...")
        logger.info(f"📊 Мониторинг {len(self.symbols)} фьючерсных пар на {len(self.timeframes)} таймфреймах")
        logger.info(f"💾 Состояние сохраняется в: {self.state_file}")
        logger.info(f"📈 SHORT сигналы: {'ВКЛЮЧЕНЫ' if self.risk_params['use_short_signals'] else 'ВЫКЛЮЧЕНЫ'}")
        logger.info(f"🎯 TP/SL рассчитываются на основе потенциала роста и индикаторов")
        
        if self.active_trades:
            logger.info(f"📥 При запуске обнаружено {len(self.active_trades)} активных сделок для отслеживания")
            for symbol, trade in self.active_trades.items():
                logger.info(f"   📌 {symbol} | {trade['signal_type']} | Вход: {trade['entry_price']}")
        
        logger.info("🕒 Цикл анализа каждую минуту")
        
        cycle_count = 0
        while True:
            try:
                cycle_count += 1
                logger.info(f"🔄 Цикл #{cycle_count}")
                
                self.run_analysis_cycle()
                
                logger.info("⏳ Ожидание следующего цикла (1 минута)...")
                time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("🛑 Бот остановлен пользователем")
                self.save_state()
                self.save_signals_log()
                logger.info("💾 Финальное состояние сохранено")
                break
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
                self.save_state()
                time.sleep(60)

# Использование бота
if __name__ == "__main__":
    bot = FuturesCryptoTradingBot()
    bot.run()