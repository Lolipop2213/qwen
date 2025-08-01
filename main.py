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
import shutil
import psutil
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
        # Список фьючерсных пар (30+ токенов) - MATIC/USDT удален
        self.symbols = [
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT',
            'ADA/USDT', 'DOGE/USDT', 'DOT/USDT', 'AVAX/USDT', 'LINK/USDT',
            'UNI/USDT', 'LTC/USDT', 'ATOM/USDT', 'ETC/USDT', 'FIL/USDT',
            'TRX/USDT', 'VET/USDT', 'XLM/USDT', 'ICP/USDT', 'FTM/USDT',
            'HBAR/USDT', 'NEAR/USDT', 'ALGO/USDT', 'EGLD/USDT', 'FLOW/USDT',
            'SAND/USDT', 'MANA/USDT', 'AXS/USDT', 'GALA/USDT', 'APE/USDT',
            'CHZ/USDT', 'ENJ/USDT', 'THETA/USDT', 'GMT/USDT'
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
        self.signal_stats = {}
        self.backtest_results = {}
        self.performance_metrics = {}
        # Кэш данных
        self.data_cache = {}
        self.cache_expiry = 300  # 5 минут
        # Имя файла для сохранения состояния
        self.state_file = 'bot_state.json'
        self.analytics_file = 'analytics_data.json'
        self.backtest_file = 'backtest_results.json'
        # Технические параметры
        self.risk_params = {
            'min_confidence_threshold': 45,  # Либеральные параметры
            'min_volume_filter': 300000,
            'min_rr_ratio': 1.2,
            'use_short_signals': True
        }
        # Коррелированные пары
        self.correlated_pairs = {
            'BTC/USDT': ['ETH/USDT', 'BNB/USDT'],
            'ETH/USDT': ['BTC/USDT', 'BNB/USDT'],
            'SOL/USDT': ['AVAX/USDT'],
            'ADA/USDT': ['DOT/USDT'],
            'DOT/USDT': ['ADA/USDT']
        }
        # Сектора активов
        self.asset_sectors = {
            'BTC/USDT': 'Bitcoin',
            'ETH/USDT': 'Ethereum',
            'BNB/USDT': 'Exchange',
            'SOL/USDT': 'Smart Contracts',
            'ADA/USDT': 'Smart Contracts',
            'DOGE/USDT': 'Meme',
            'DOT/USDT': 'Smart Contracts',
            'AVAX/USDT': 'Smart Contracts',
            'LINK/USDT': 'Oracle',
            'UNI/USDT': 'DEX',
            'LTC/USDT': 'Payments',
            'ATOM/USDT': 'Interoperability',
            'ETC/USDT': 'Payments',
            'FIL/USDT': 'Storage',
            'TRX/USDT': 'Entertainment',
            'VET/USDT': 'Supply Chain',
            'XLM/USDT': 'Payments',
            'ICP/USDT': 'Infrastructure',
            'FTM/USDT': 'Smart Contracts',
            'HBAR/USDT': 'Payments',
            'NEAR/USDT': 'Smart Contracts',
            'ALGO/USDT': 'Smart Contracts',
            'EGLD/USDT': 'Payments',
            'FLOW/USDT': 'NFT',
            'SAND/USDT': 'Metaverse',
            'MANA/USDT': 'Metaverse',
            'AXS/USDT': 'Gaming',
            'GALA/USDT': 'Gaming',
            'APE/USDT': 'NFT',
            'CHZ/USDT': 'Sports',
            'ENJ/USDT': 'NFT',
            'THETA/USDT': 'Video',
            'GMT/USDT': 'Social'
        }
        # Загрузка состояния при инициализации
        self.load_state()
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

    def fetch_ohlcv_with_cache(self, symbol, timeframe, limit=100):
        """Получение данных с кэшированием"""
        cache_key = f"{symbol}_{timeframe}_{limit}"
        current_time = time.time()
        if cache_key in self.data_cache:
            cached_data, timestamp = self.data_cache[cache_key]
            if current_time - timestamp < self.cache_expiry:
                return cached_data
        # Получаем свежие данные
        data = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if len(data) > 0:
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            self.data_cache[cache_key] = (df, current_time)
            return df
        else:
            self.data_cache[cache_key] = (None, current_time)
            return None

    def fetch_ohlcv_multitimeframe(self, symbol):
        """Получение данных по нескольким таймфреймам"""
        data = {}
        try:
            for tf in self.timeframes:
                limit = 200 if tf in ['5m', '15m'] else 100
                df = self.fetch_ohlcv_with_cache(symbol, tf, limit=limit)
                data[tf] = df
        except Exception as e:
            logger.error(f"Ошибка получения данных для {symbol}: {e}")
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
            # Паттерны свечей
            df['candle_body'] = abs(df['close'] - df['open'])
            df['candle_ratio'] = df['candle_body'] / (df['high'] - df['low'] + 0.0001)
            # Тренд
            df['price_trend_20'] = (df['close'] - df['close'].shift(20)) / (df['close'].shift(20) + 0.0001)
            df['price_trend_50'] = (df['close'] - df['close'].shift(50)) / (df['close'].shift(50) + 0.0001)
            # Волатильность
            df['volatility'] = df['close'].pct_change().rolling(window=14).std() * np.sqrt(365)
            # Свинг хай/лоу
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
            # Пинбары и другие паттерны
            df['body'] = abs(df['close'] - df['open'])
            df['upper_wick'] = df['high'] - df[['open', 'close']].max(axis=1)
            df['lower_wick'] = df[['open', 'close']].min(axis=1) - df['low']
            df['wick_ratio'] = df['body'] / (df['high'] - df['low'] + 0.0001)
        except Exception as e:
            logger.error(f"Ошибка расчета индикаторов: {e}")
            return df
        return df

    def detect_advanced_candlestick_patterns(self, df):
        """Обнаружение расширенных паттернов свечей"""
        if df is None or len(df) < 10:
            return {}
        patterns = {}
        try:
            # PINBAR (пинцет)
            if len(df) >= 2:
                current = df.iloc[-1]
                prev = df.iloc[-2]
                body = abs(current['close'] - current['open'])
                total_range = current['high'] - current['low']
                upper_wick = current['high'] - max(current['close'], current['open'])
                lower_wick = min(current['close'], current['open']) - current['low']
                # Пинбар на поддержке/сопротивлении
                if total_range > 0 and body/total_range < 0.3:  # Маленькое тело
                    if upper_wick > body * 2 and lower_wick < body * 0.5:  # Длинный верхний фитиль
                        patterns['pinbar_bearish'] = True
                    elif lower_wick > body * 2 and upper_wick < body * 0.5:  # Длинный нижний фитиль
                        patterns['pinbar_bullish'] = True
            # DOJI
            if len(df) >= 1:
                current = df.iloc[-1]
                body = abs(current['close'] - current['open'])
                total_range = current['high'] - current['low']
                if total_range > 0 and body/total_range < 0.1:
                    patterns['doji'] = True
            # SPINNING TOP
            if len(df) >= 1:
                current = df.iloc[-1]
                body = abs(current['close'] - current['open'])
                total_range = current['high'] - current['low']
                if total_range > 0 and 0.1 <= body/total_range <= 0.3:
                    patterns['spinning_top'] = True
            # THREE WHITE SOLDIERS (три белых солдата)
            if len(df) >= 3:
                soldier1 = df.iloc[-3]
                soldier2 = df.iloc[-2]
                soldier3 = df.iloc[-1]
                if (soldier1['close'] > soldier1['open'] and  # Все зеленые
                    soldier2['close'] > soldier2['open'] and
                    soldier3['close'] > soldier3['open'] and
                    soldier3['close'] > soldier2['close'] > soldier1['close'] and  # Каждая выше предыдущей
                    soldier2['open'] > soldier1['open'] and soldier3['open'] > soldier2['open']):  # Каждая открывается выше предыдущей
                    patterns['three_white_soldiers'] = True
            # THREE BLACK CROWS (три черных ворона)
            if len(df) >= 3:
                crow1 = df.iloc[-3]
                crow2 = df.iloc[-2]
                crow3 = df.iloc[-1]
                if (crow1['close'] < crow1['open'] and  # Все красные
                    crow2['close'] < crow2['open'] and
                    crow3['close'] < crow3['open'] and
                    crow3['close'] < crow2['close'] < crow1['close'] and  # Каждая ниже предыдущей
                    crow2['open'] < crow1['open'] and crow3['open'] < crow2['open']):  # Каждая открывается ниже предыдущей
                    patterns['three_black_crows'] = True
            # MORNING STAR (утренняя звезда)
            if len(df) >= 3:
                first = df.iloc[-3]  # Красная свеча
                second = df.iloc[-2]  # Маленькая свеча
                third = df.iloc[-1]   # Зеленая свеча
                body_first = abs(first['close'] - first['open'])
                body_second = abs(second['close'] - second['open'])
                body_third = abs(third['close'] - third['open'])
                if (first['close'] < first['open'] and  # Первая красная
                    body_second < body_first * 0.5 and  # Вторая маленькая
                    third['close'] > third['open'] and   # Третья зеленая
                    third['close'] > first['open']):     # Третья закрывается выше первой
                    patterns['morning_star'] = True
            # EVENING STAR (вечерняя звезда)
            if len(df) >= 3:
                first = df.iloc[-3]  # Зеленая свеча
                second = df.iloc[-2]  # Маленькая свеча
                third = df.iloc[-1]   # Красная свеча
                body_first = abs(first['close'] - first['open'])
                body_second = abs(second['close'] - second['open'])
                body_third = abs(third['close'] - third['open'])
                if (first['close'] > first['open'] and  # Первая зеленая
                    body_second < body_first * 0.5 and  # Вторая маленькая
                    third['close'] < third['open'] and   # Третья красная
                    third['close'] < first['open']):     # Третья закрывается ниже первой
                    patterns['evening_star'] = True
            # HARAMI (внутренняя свеча)
            if len(df) >= 2:
                parent = df.iloc[-2]  # Родительская свеча
                child = df.iloc[-1]   # Дочерняя свеча
                if (parent['close'] > parent['open'] and  # Родитель зеленый
                    child['close'] < child['open'] and    # Дочерняя красная
                    child['high'] < parent['high'] and    # Дочерняя внутри родителя
                    child['low'] > parent['low']):
                    patterns['bearish_harami'] = True
                elif (parent['close'] < parent['open'] and  # Родитель красный
                      child['close'] > child['open'] and    # Дочерняя зеленая
                      child['high'] < parent['high'] and    # Дочерняя внутри родителя
                      child['low'] > parent['low']):
                    patterns['bullish_harami'] = True
            # ENGULFING (поглощение)
            if len(df) >= 2:
                first = df.iloc[-2]  # Первая свеча
                second = df.iloc[-1]  # Вторая свеча
                # BULLISH ENGULFING (бычье поглощение)
                if (first['close'] < first['open'] and  # Первая красная
                    second['close'] > second['open'] and  # Вторая зеленая
                    second['close'] > first['open'] and    # Закрытие второй выше открытия первой
                    second['open'] < first['close']):      # Открытие второй ниже закрытия первой
                    patterns['bullish_engulfing'] = True
                # BEARISH ENGULFING (медвежье поглощение)
                elif (first['close'] > first['open'] and  # Первая зеленая
                      second['close'] < second['open'] and  # Вторая красная
                      second['close'] < first['open'] and   # Закрытие второй ниже открытия первой
                      second['open'] > first['close']):     # Открытие второй выше закрытия первой
                    patterns['bearish_engulfing'] = True
            # HAMMER (молот)
            if len(df) >= 1:
                current = df.iloc[-1]
                body = abs(current['close'] - current['open'])
                upper_wick = current['high'] - max(current['close'], current['open'])
                lower_wick = min(current['close'], current['open']) - current['low']
                if (lower_wick > body * 2 and  # Длинный нижний фитиль
                    upper_wick < body * 0.5 and  # Короткий верхний фитиль
                    body > 0):
                    patterns['hammer'] = True
            # SHOOTING STAR (падающая звезда)
            if len(df) >= 1:
                current = df.iloc[-1]
                body = abs(current['close'] - current['open'])
                upper_wick = current['high'] - max(current['close'], current['open'])
                lower_wick = min(current['close'], current['open']) - current['low']
                if (upper_wick > body * 2 and  # Длинный верхний фитиль
                    lower_wick < body * 0.5 and  # Короткий нижний фитиль
                    body > 0):
                    patterns['shooting_star'] = True
            # TWEZZER TOPS/BOTTOMS (щипцы)
            if len(df) >= 2:
                first = df.iloc[-2]
                second = df.iloc[-1]
                # TWEZZER BOTTOMS
                if (abs(first['low'] - second['low']) < (first['high'] - first['low']) * 0.05 and  # Почти одинаковые минимумы
                    first['close'] < first['open'] and second['close'] > second['open']):  # Первая красная, вторая зеленая
                    patterns['tweezer_bottoms'] = True
                # TWEZZER TOPS
                elif (abs(first['high'] - second['high']) < (first['high'] - first['low']) * 0.05 and  # Почти одинаковые максимумы
                      first['close'] > first['open'] and second['close'] < second['open']):  # Первая зеленая, вторая красная
                    patterns['tweezer_tops'] = True
            # GAP UP/DOWN (гэпы)
            if len(df) >= 2:
                prev = df.iloc[-2]
                current = df.iloc[-1]
                # GAP UP
                if current['low'] > prev['high']:
                    patterns['gap_up'] = True
                # GAP DOWN
                elif current['high'] < prev['low']:
                    patterns['gap_down'] = True
            # INSIDE BAR (внутренняя свеча)
            if len(df) >= 2:
                parent = df.iloc[-2]
                child = df.iloc[-1]
                if (child['high'] < parent['high'] and  # Дочерняя внутри родительской
                    child['low'] > parent['low']):
                    patterns['inside_bar'] = True
            # OUTSIDE BAR (внешняя свеча)
            if len(df) >= 2:
                parent = df.iloc[-2]
                child = df.iloc[-1]
                if (child['high'] > parent['high'] and  # Дочерняя охватывает родительскую
                    child['low'] < parent['low']):
                    patterns['outside_bar'] = True
        except Exception as e:
            logger.error(f"Ошибка обнаружения паттернов: {e}")
        return patterns

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
            # Анализ потенциала роста на основе индикаторов
            rsi = float(df_1h['rsi'].iloc[-1]) if not pd.isna(df_1h['rsi'].iloc[-1]) else 50
            bb_position = float(df_1h['bb_position'].iloc[-1]) if not pd.isna(df_1h['bb_position'].iloc[-1]) else 0.5
            momentum_1h = float(df_1h['roc_7'].iloc[-1]) if not pd.isna(df_1h['roc_7'].iloc[-1]) else 0
            volume_ratio = float(df_1h['volume_ratio'].iloc[-1]) if not pd.isna(df_1h['volume_ratio'].iloc[-1]) else 1
            # Анализ тренда на старших таймфреймах
            trend_strength_4h = 0
            trend_strength_1h = 0
            if '4h' in data_dict and data_dict['4h'] is not None and len(data_dict['4h']) > 20:
                df_4h = data_dict['4h']
                trend_4h = (df_4h['close'].iloc[-1] - df_4h['close'].iloc[-20]) / df_4h['close'].iloc[-20]
                trend_strength_4h = float(trend_4h) if not pd.isna(trend_4h) else 0
            if '1h' in data_dict and data_dict['1h'] is not None and len(data_dict['1h']) > 20:
                df_1h_local = data_dict['1h']
                trend_1h = (df_1h_local['close'].iloc[-1] - df_1h_local['close'].iloc[-20]) / df_1h_local['close'].iloc[-20]
                trend_strength_1h = float(trend_1h) if not pd.isna(trend_1h) else 0
            # Расчет множителя потенциала на основе индикаторов
            potential_multiplier = 1.0
            if signal_type == 'LONG':
                # RSI фактор (чем ниже RSI, тем больше потенциал роста)
                rsi_factor = max(0.7, min(1.5, (70 - rsi) / 30))  # 0.7 - 1.5
                # BB фактор (позиция цены в канале Боллинджера)
                bb_factor = max(0.7, min(1.5, (1.0 - bb_position) * 1.8))  # 0.7 - 1.5
                # Моментум фактор
                momentum_factor = max(0.8, min(1.3, 1.0 + momentum_1h * 10))  # 0.8 - 1.3
                # Тренд фактор
                trend_factor = max(0.8, min(1.3, 1.0 + (trend_strength_4h + trend_strength_1h) * 5))  # 0.8 - 1.3
                # Объем фактор
                volume_factor = max(0.9, min(1.2, volume_ratio * 0.3 + 0.8))  # 0.9 - 1.2
                potential_multiplier = (rsi_factor + bb_factor + momentum_factor + trend_factor + volume_factor) / 5
                potential_multiplier = max(0.8, min(1.5, potential_multiplier))  # Ограничиваем 0.8 - 1.5
            else:  # SHORT
                # RSI фактор (чем выше RSI, тем больше потенциал падения)
                rsi_factor = max(0.7, min(1.5, (rsi - 30) / 30))  # 0.7 - 1.5
                # BB фактор (позиция цены в канале Боллинджера)
                bb_factor = max(0.7, min(1.5, bb_position * 1.8))  # 0.7 - 1.5
                # Моментум фактор
                momentum_factor = max(0.8, min(1.3, 1.0 - momentum_1h * 10))  # 0.8 - 1.3
                # Тренд фактор
                trend_factor = max(0.8, min(1.3, 1.0 - (trend_strength_4h + trend_strength_1h) * 5))  # 0.8 - 1.3
                # Объем фактор
                volume_factor = max(0.9, min(1.2, volume_ratio * 0.3 + 0.8))  # 0.9 - 1.2
                potential_multiplier = (rsi_factor + bb_factor + momentum_factor + trend_factor + volume_factor) / 5
                potential_multiplier = max(0.8, min(1.5, potential_multiplier))  # Ограничиваем 0.8 - 1.5
            # Расчет уровней с учетом потенциала
            base_sl_distance = atr * 1.3
            base_tp1_distance = atr * 2.0
            base_tp2_distance = atr * 3.5
            base_tp3_distance = atr * 5.0
            if signal_type == 'LONG':
                sl = current_price - (base_sl_distance * 0.9)  # SL немного уменьшаем
                tp1 = current_price + (base_tp1_distance * potential_multiplier)
                tp2 = current_price + (base_tp2_distance * potential_multiplier * 1.1)  # TP2 чуть больше
                tp3 = current_price + (base_tp3_distance * potential_multiplier * 1.2)  # TP3 еще больше
                # Проверка уровней поддержки/сопротивления
                resistance_levels = self.find_resistance_levels(df_1h, current_price)
                if resistance_levels:
                    nearest_resistance = min(resistance_levels)
                    if tp3 > nearest_resistance * 0.99:  # Не выше уровня сопротивления
                        tp3 = nearest_resistance * 0.99
                    if tp2 > nearest_resistance * 0.995:
                        tp2 = nearest_resistance * 0.995
                    if tp1 > nearest_resistance * 0.998:
                        tp1 = nearest_resistance * 0.998
            else:  # SHORT
                sl = current_price + (base_sl_distance * 0.9)  # SL немного уменьшаем
                tp1 = current_price - (base_tp1_distance * potential_multiplier)
                tp2 = current_price - (base_tp2_distance * potential_multiplier * 1.1)  # TP2 чуть больше
                tp3 = current_price - (base_tp3_distance * potential_multiplier * 1.2)  # TP3 еще больше
                # Проверка уровней поддержки/сопротивления
                support_levels = self.find_support_levels(df_1h, current_price)
                if support_levels:
                    nearest_support = max(support_levels)
                    if tp3 < nearest_support * 1.01:  # Не ниже уровня поддержки
                        tp3 = nearest_support * 1.01
                    if tp2 < nearest_support * 1.005:
                        tp2 = nearest_support * 1.005
                    if tp1 < nearest_support * 1.002:
                        tp1 = nearest_support * 1.002
            # Проверка валидности уровней
            risk_reward_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
            if signal_type == 'LONG' and sl < current_price and tp3 > current_price and risk_reward_ratio > 1.2:
                valid = True
            elif signal_type == 'SHORT' and sl > current_price and tp3 < current_price and risk_reward_ratio > 1.2:
                valid = True
            else:
                valid = False
            if valid:
                return round(float(sl), 8), round(float(tp1), 8), round(float(tp2), 8), round(float(tp3), 8)
            else:
                return self.calculate_basic_levels(symbol, data_dict, signal_type)
        except Exception as e:
            logger.error(f"Ошибка расчета динамических уровней для {symbol}: {e}")
            return self.calculate_basic_levels(symbol, data_dict, signal_type)

    def find_resistance_levels(self, df, current_price):
        """Поиск уровней сопротивления"""
        if df is None or len(df) < 20:
            return []
        try:
            # Свинг хай
            swing_highs = df['swing_high'].dropna().tail(10)
            resistance_from_swing = list(swing_highs.values) if len(swing_highs) > 0 else []
            # Пивотные уровни
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
            # Пивотные уровни
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

    def calculate_basic_levels(self, symbol, data_dict, signal_type):
        """Базовые уровни на случай ошибок"""
        try:
            if not data_dict or '1h' not in data_dict or data_dict['1h'] is None:
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
            return round(float(sl), 8), round(float(tp1), 8), round(float(tp2), 8), round(float(tp3), 8)
        except Exception as e:
            logger.error(f"Ошибка базовых уровней для {symbol}: {e}")
            # Абсолютный fallback
            current_price = 1000.0
            if signal_type == 'LONG':
                return 990.0, 1005.0, 1015.0, 1025.0
            else:
                return 1010.0, 995.0, 985.0, 975.0

    # Добавленный метод для исправления ошибки
    def calculate_multitimeframe_analysis(self, data_dict):
        """
        Заглушка для анализа по нескольким таймфреймам.
        Возвращает базовый словарь или None.
        TODO: Реализовать реальную логику анализа.
        """
        # logger.debug("calculate_multitimeframe_analysis вызван, но не реализован.")
        # Возвращаем пустой словарь или можно вернуть data_dict, если метод просто для структуры.
        # Или None, если метод не критичен для generate_signal.
        return {} # Или return None или return data_dict в зависимости от использования в generate_signal

    # Улучшенная версия метода generate_signal
    def generate_signal(self, symbol, data_dict):
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
            
            # Анализ тренда на старших таймфреймах для более точного фильтра
            trend_1h = 0
            trend_4h = 0
            if '1h' in data_dict and data_dict['1h'] is not None and len(data_dict['1h']) > 20:
                df_1h_trend = data_dict['1h']
                trend_1h = (df_1h_trend['close'].iloc[-1] - df_1h_trend['close'].iloc[-20]) / df_1h_trend['close'].iloc[-20]
                trend_1h = float(trend_1h) if not pd.isna(trend_1h) else 0
            if '4h' in data_dict and data_dict['4h'] is not None and len(data_dict['4h']) > 20:
                df_4h_trend = data_dict['4h']
                trend_4h = (df_4h_trend['close'].iloc[-1] - df_4h_trend['close'].iloc[-20]) / df_4h_trend['close'].iloc[-20]
                trend_4h = float(trend_4h) if not pd.isna(trend_4h) else 0

            # Уточненные условия для LONG (Более строгие)
            long_conditions = [
                rsi < 35,  # Более глубокая перепроданность
                stoch_k < 30 and stoch_d < 30,  # Более глубокие уровни стохастика
                macd > macd_signal,  # MACD bullish
                bb_position < 0.2,  # Цена ближе к нижней границе Боллинджера
                volume_ratio > 1.1,  # Увеличение объема
                momentum_1h > -0.01,   # Умеренный downtrend или нейтральный
                trend_4h > 0.005,      # Тренд на 4h должен быть явно восходящим
                trend_1h > -0.01       # Тренд на 1h не должен быть сильно нисходящим
            ]
            # Уточненные условия для SHORT (Более строгие)
            short_conditions = [
                rsi > 65,  # Более глубокая перекупленность
                stoch_k > 70 and stoch_d > 70,  # Более глубокие уровни стохастика
                macd < macd_signal,  # MACD bearish
                bb_position > 0.8,  # Цена ближе к верхней границе Боллинджера
                volume_ratio > 1.1,  # Увеличение объема
                momentum_1h < 0.01,   # Умеренный uptrend или нейтральный
                trend_4h < -0.005,     # Тренд на 4h должен быть явно нисходящим
                trend_1h < 0.01        # Тренд на 1h не должен быть сильно восходящим
            ]

            # --- ИСПРАВЛЕННАЯ ЛОГИКА ВЫБОРА ЛУЧШЕГО СИГНАЛА ---
            # Сначала вычисляем оба счетчика
            long_score = sum(1 for cond in long_conditions if cond) # Считаем количество выполненных условий
            short_score = sum(1 for cond in short_conditions if cond) # Считаем количество выполненных условий

            signal_type = None
            confidence_score = 0

            # Проверяем, разрешены ли SHORT сигналы
            # (Мы это делаем здесь, чтобы корректно сравнить счетчики позже)
            effective_short_score = short_score if self.risk_params['use_short_signals'] else -1 # Если SHORT запрещены, считаем их счетчик недействительным

            # Теперь выбираем лучший сигнал на основе счетчиков
            # Условия:
            # 1. Счетчик должен быть >= 5 (повышенный порог)
            # 2. Счетчик должен быть строго больше счетчика противоположного направления
            # 3. Если счетчики равны и >= 5, выбираем LONG.
            
            if long_score >= 5 and short_score >= 5:
                # Оба сигнала подходят по порогу, выбираем с более высоким счетчиком
                if long_score > short_score:
                    signal_type = 'LONG'
                    confidence_score = (long_score / len(long_conditions)) * 100
                elif short_score > long_score and self.risk_params['use_short_signals']:
                    signal_type = 'SHORT'
                    confidence_score = (short_score / len(short_conditions)) * 100
                # Если long_score == short_score, выбираем LONG по умолчанию
                elif long_score == short_score:
                     signal_type = 'LONG'
                     confidence_score = (long_score / len(long_conditions)) * 100
                     
            elif long_score >= 5:
                # Только LONG подходит
                signal_type = 'LONG'
                confidence_score = (long_score / len(long_conditions)) * 100
            elif short_score >= 5 and self.risk_params['use_short_signals']:
                # Только SHORT подходит
                signal_type = 'SHORT'
                confidence_score = (short_score / len(short_conditions)) * 100

            # --- КОНЕЦ ИСПРАВЛЕННОЙ ЛОГИКИ ---

            # Сила сигнала (1-5) - рассчитывается после выбора сигнала
            signal_strength = 1 # По умолчанию
            if signal_type:
                selected_score = long_score if signal_type == 'LONG' else short_score
                selected_total = len(long_conditions) if signal_type == 'LONG' else len(short_conditions)
                
                if selected_score >= 7: # 7 из 8
                    signal_strength = 5
                elif selected_score >= 6: # 6 из 8
                    signal_strength = 4
                elif selected_score >= 5: # 5 из 8
                    signal_strength = 3
                # signal_strength 1 и 2 для счетчиков < 5 уже не применяются, так как сигнал не генерируется
                
            # Порог уверенности 60% (проверяется после выбора типа сигнала)
            if signal_type and confidence_score >= 60: 
                # Обнаружение паттернов свечей
                patterns = self.detect_advanced_candlestick_patterns(df_1h)
                # Если найдены сильные паттерны, увеличиваем силу сигнала
                strong_patterns = [
                    'bullish_engulfing', 'bearish_engulfing', 'hammer', 'shooting_star',
                    'three_white_soldiers', 'three_black_crows', 'morning_star', 'evening_star'
                ]
                # Паттерны как обязательное условие или подтверждение
                if signal_type == 'LONG' and not any(pattern in patterns for pattern in ['bullish_engulfing', 'hammer', 'morning_star', 'three_white_soldiers']):
                    # Если нет сильных бычьих паттернов, уменьшаем уверенность
                    confidence_score *= 0.8
                elif signal_type == 'SHORT' and not any(pattern in patterns for pattern in ['bearish_engulfing', 'shooting_star', 'evening_star', 'three_black_crows']):
                    # Если нет сильных медвежьих паттернов, уменьшаем уверенность
                    confidence_score *= 0.8

                # Рассчитываем динамические TP и SL
                sl, tp1, tp2, tp3 = self.calculate_dynamic_levels(symbol, data_dict, signal_type)
                if sl is None or tp1 is None or tp2 is None or tp3 is None:
                    logger.debug(f"❌ {symbol}: Ошибка расчета уровней")
                    return None
                # Проверка валидности уровней
                risk_reward_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
                # RR ratio
                if signal_type == 'LONG' and sl < current_price and tp3 > current_price and risk_reward_ratio > 1.5:
                    valid = True
                elif signal_type == 'SHORT' and sl > current_price and tp3 < current_price and risk_reward_ratio > 1.5:
                    valid = True
                else:
                    valid = False
                if valid:
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
                        'signal_strength': int(signal_strength),
                        'patterns': list(patterns.keys()),  # Сохраняем найденные паттерны
                        'timeframe_analysis': {
                            '5m_momentum': round(float(momentum_5m), 6),
                            '15m_momentum': round(float(momentum_15m), 6),
                            '1h_momentum': round(float(momentum_1h), 6),
                            '1h_trend': round(float(trend_1h), 6),
                            '4h_trend': round(float(trend_4h), 6)
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
            logger.error(f"Ошибка генерации сигнала для {symbol}: {e}")
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

    # Исправленный метод send_signal
    def send_signal(self, signal):
        """Отправка торгового сигнала"""
        if signal is None:
            return
        symbol = signal['symbol']
        
        # НОВАЯ ЛОГИКА: Проверяем, есть ли уже активная сделка по этой паре.
        # Это предотвратит генерацию новых сигналов, пока позиция открыта.
        # Логика "один сигнал в час" убрана.
        if symbol in self.active_trades:
             # Сигнал не отправляется, если сделка уже открыта.
             # logger.debug(f"⏭️  Сигнал для {symbol} не отправлен - позиция уже открыта.")
             return # Просто выходим из функции

        # Если активной сделки нет, обрабатываем и отправляем сигнал
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
            'signal_strength': int(signal['signal_strength']),
            'patterns': signal.get('patterns', [])
        }
        self.signals_found.append(self.convert_to_serializable(signal_log_entry))
        self.analysis_stats['signals_generated'] += 1
        logger.info(f"✅ [{signal['symbol']}] --- ОТКРЫТА {signal['signal_type']} СДЕЛКА (Сила: {signal['signal_strength']}) @ {signal['entry_price']} ---")
        logger.info(f"   SL: {signal['sl']}, TP1: {signal['tp1']}, TP2: {signal['tp2']}, TP3: {signal['tp3']}")
        logger.info(f"   📈 Потенциал: {signal.get('potential_upside', 0):.1f}% | RR: {signal['risk_reward_ratio']}")
        # Показываем найденные паттерны
        if signal.get('patterns'):
            logger.info(f"   🔍 Паттерны: {', '.join(signal['patterns'])}")
        
        # Добавляем сделку в активные (это уже было, но теперь без предварительной проверки symbol not in self.active_trades)
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

    # Исправленный метод process_symbol
    def process_symbol(self, symbol):
        """Обработка одного символа"""
        try:
            data_dict = self.fetch_ohlcv_multitimeframe(symbol)
            if not data_dict:
                return None
            for tf in self.timeframes:
                if tf in data_dict and data_dict[tf] is not None:
                    data_dict[tf] = self.calculate_advanced_indicators(data_dict[tf], tf)
            # Исправление ошибки: calculate_multitimeframe_analysis теперь определен
            multitimeframe_analysis = self.calculate_multitimeframe_analysis(data_dict)
            if symbol not in self.active_trades:
                # Убираем multitimeframe_analysis из вызова, так как он не используется
                signal = self.generate_signal(symbol, data_dict) 
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
        # Изменено: Цикл анализа каждую минуту вместо 5
        logger.info(f"🕒 Цикл анализа каждую минуту")
        if self.active_trades:
            logger.info(f"📥 При запуске обнаружено {len(self.active_trades)} активных сделок для отслеживания")
            for symbol, trade in self.active_trades.items():
                logger.info(f"   📌 {symbol} | {trade['signal_type']} | Вход: {trade['entry_price']}")
        cycle_count = 0
        while True:
            try:
                cycle_count += 1
                logger.info(f"🔄 Цикл #{cycle_count}")
                self.run_analysis_cycle()
                # Изменено: Ожидание 60 секунд вместо 300
                logger.info("⏳ Ожидание следующего цикла (1 минута)...")
                time.sleep(60)  # 1 минута
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