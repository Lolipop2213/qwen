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

# Создаем основной логгер
logger = logging.getLogger(__name__)
# Устанавливаем общий уровень на DEBUG, чтобы логгер обрабатывал все сообщения
logger.setLevel(logging.DEBUG)
# Создаем формат
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format)
# --- Обработчик для файла (только INFO и выше) ---
file_handler = logging.FileHandler('crypto_futures_bot.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)  # <-- Устанавливаем уровень INFO для файла
file_handler.setFormatter(formatter)
# --- Обработчик для консоли (все сообщения, включая DEBUG) ---
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG) # <-- Устанавливаем уровень DEBUG для консоли
console_handler.setFormatter(formatter)
# Очищаем существующие обработчики (на случай, если логгер уже использовался)
logger.handlers.clear()
# Добавляем обработчики к логгеру
logger.addHandler(file_handler)
logger.addHandler(console_handler)
# Отключаем propagate, чтобы избежать дублирования через корневой логгер
logger.propagate = False

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
            'CHZ/USDT', 'ENJ/USDT', 'THETA/USDT', 'GMT/USDT', '1000PEPE/USDT',
            'SUI/USDT', 'JUP/USDT', 'WLD/USDT', 'INJ/USDT', 'TIA/USDT',
            'STRK/USDT', 'SEI/USDT', 'PYTH/USDT', 'JTO/USDT', 'APT/USDT',
            'FET/USDT', 'AGIX/USDT', 'OP/USDT', 'ARB/USDT', 'AAVE/USDT', 
            'LDO/USDT', 'ENS/USDT', 'MKR/USDT',
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
        # --- НОВОЕ: Хранилище для лога закрытия сделок ---
        self.trade_closures_log = [] # Список словарей с информацией о закрытиях
        # --- КОНЕЦ НОВОГО ---
        # Кэш данных
        self.data_cache = {}
        self.cache_expiry = 300  # 5 минут
        # Имя файла для сохранения состояния
        self.state_file = 'bot_state.json'
        # Технические параметры
        self.risk_params = {
            'min_confidence_threshold': 45,  # Либеральные параметры
            'min_volume_filter': 250000,
            'min_rr_ratio': 1.2,
            'use_short_signals': True,
            'atr_fallback_pct': 0.01,
            'sl_mult': 1.5,
            'sl_scale': 0.9,
            'tp1_mult': 2.0,
            'tp2_mult': 3.5,
            'tp3_mult': 5.5,
            'min_level_dist_pct': 0.001,
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
                    #logger.info(f"📥 Восстановлено {len(self.active_trades)} активных сделок из состояния")
                if 'signal_history' in state:
                    raw_history = state['signal_history']
                    self.signal_history = defaultdict(list)
                    for symbol, signals in raw_history.items():
                        self.signal_history[symbol] = [self.convert_to_serializable(signal) for signal in signals]
                    total_signals = sum(len(signals) for signals in self.signal_history.values())
                    logger.info(f"📥 Восстановлено {total_signals} сигналов из истории")
                if 'signals_found' in state:
                    self.signals_found = [self.convert_to_serializable(signal) for signal in state['signals_found']]
                    #logger.info(f"📥 Восстановлено {len(self.signals_found)} найденных сигналов")
                if 'analysis_stats' in state:
                    self.analysis_stats = self.convert_to_serializable(state['analysis_stats'])
                    #logger.info("📥 Статистика восстановлена")
                # --- НОВОЕ: Загрузка лога закрытий ---
                if 'trade_closures_log' in state:
                    raw_closures = state['trade_closures_log']
                    self.trade_closures_log = [self.convert_to_serializable(entry) for entry in raw_closures]
                    logger.info(f"📥 Восстановлено {len(self.trade_closures_log)} записей о закрытиях из состояния")
                # --- КОНЕЦ НОВОГО ---
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
                # --- НОВОЕ: Инициализация лога закрытий в дефолтном состоянии ---
                'trade_closures_log': [],
                # --- КОНЕЦ НОВОГО ---
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
                # --- НОВОЕ: Сохранение лога закрытий ---
                'trade_closures_log': self.convert_to_serializable(self.trade_closures_log),
                # --- КОНЕЦ НОВОГО ---
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

    def calculate_multitimeframe_analysis(self, data_dict):
        """
        Анализирует согласованность трендов и импульса на разных таймфреймах.
        Возвращает словарь с результатами анализа.
        """
        if not data_dict:
            return {}
        analysis_results = {
            'trend_consistency': 'neutral', # 'strong_long', 'long', 'neutral', 'short', 'strong_short'
            'momentum_alignment': 'neutral', # 'aligned_long', 'aligned_short', 'divergent', 'neutral'
            'volatility_regime': 'normal', # 'low', 'normal', 'high'
            'timeframe_agreement_score': 0 # 0-100, где 100 - полное согласие
        }
        try:
            # --- 1. Анализ согласованности тренда ---
            trend_signals = []
            # Получаем тренды с разных таймфреймов
            # Предполагаем, что индикаторы уже рассчитаны в calculate_advanced_indicators
            for tf in ['15m', '1h', '4h']: # Анализируем старшие таймфреймы
                if tf in data_dict and data_dict[tf] is not None and len(data_dict[tf]) > 20:
                    df = data_dict[tf]
                    # Простой тренд: сравнение цены закрытия с 20-периодной MA или ценой 20 баров назад
                    # Используем price_trend_20, который уже рассчитан
                    if 'price_trend_20' in df.columns:
                        trend_val = df['price_trend_20'].iloc[-1]
                        if not pd.isna(trend_val):
                            if trend_val > 0.01: # Порог для подтверждения тренда, например, 1%
                                trend_signals.append(1) # Long
                            elif trend_val < -0.01:
                                trend_signals.append(-1) # Short
                            else:
                                trend_signals.append(0) # Neutral/Flat
            # Определяем согласованность тренда
            if len(trend_signals) >= 2: # Нужно хотя бы 2 таймфрейма
                long_votes = sum(1 for t in trend_signals if t == 1)
                short_votes = sum(1 for t in trend_signals if t == -1)
                neutral_votes = sum(1 for t in trend_signals if t == 0)
                if long_votes == len(trend_signals): # Все таймфреймы дают сигнал Long
                    analysis_results['trend_consistency'] = 'strong_long'
                elif short_votes == len(trend_signals): # Все таймфреймы дают сигнал Short
                    analysis_results['trend_consistency'] = 'strong_short'
                elif long_votes > short_votes and long_votes >= 2: # Большинство Long
                    analysis_results['trend_consistency'] = 'long'
                elif short_votes > long_votes and short_votes >= 2: # Большинство Short
                    analysis_results['trend_consistency'] = 'short'
                # В остальных случаях остается 'neutral'
            # --- 2. Анализ согласованности импульса ---
            momentum_signals = []
            for tf in ['5m', '15m', '1h']:
                 if tf in data_dict and data_dict[tf] is not None and len(data_dict[tf]) > 3:
                    df = data_dict[tf]
                    if 'roc_3' in df.columns: # Используем ROC как меру импульса
                        mom_val = df['roc_3'].iloc[-1]
                        if not pd.isna(mom_val):
                             if mom_val > 0.005: # Порог импульса, например, 0.5%
                                momentum_signals.append(1) # Positive momentum
                             elif mom_val < -0.005:
                                momentum_signals.append(-1) # Negative momentum
                             # else: близко к нулю, не добавляем
            # Определяем согласованность импульса
            if len(momentum_signals) >= 2:
                 pos_mom_votes = sum(1 for m in momentum_signals if m == 1)
                 neg_mom_votes = sum(1 for m in momentum_signals if m == -1)
                 if pos_mom_votes == len(momentum_signals):
                      analysis_results['momentum_alignment'] = 'aligned_long'
                 elif neg_mom_votes == len(momentum_signals):
                      analysis_results['momentum_alignment'] = 'aligned_short'
                 elif pos_mom_votes > 0 and neg_mom_votes > 0:
                      analysis_results['momentum_alignment'] = 'divergent' # Конфликт импульсов
                 # else: остается neutral
            # --- 3. Анализ волатильности ---
            # Используем 'volatility' с таймфрейма 1h как основную меру
            if '1h' in data_dict and data_dict['1h'] is not None and len(data_dict['1h']) > 14:
                df_1h = data_dict['1h']
                if 'volatility' in df_1h.columns:
                    current_vol = df_1h['volatility'].iloc[-1]
                    avg_vol = df_1h['volatility'].iloc[-14:].mean() # Средняя за 14 периодов
                    if not pd.isna(current_vol) and not pd.isna(avg_vol) and avg_vol > 0:
                         vol_ratio = current_vol / avg_vol
                         if vol_ratio > 1.5:
                              analysis_results['volatility_regime'] = 'high'
                         elif vol_ratio < 0.7:
                              analysis_results['volatility_regime'] = 'low'
                         # else: остается normal
            # --- 4. Расчет общего счета согласия ---
            score = 50 # Базовый счет
            # Добавляем баллы за согласованный тренд
            if analysis_results['trend_consistency'] == 'strong_long':
                score += 15
            elif analysis_results['trend_consistency'] == 'long':
                score += 7
            elif analysis_results['trend_consistency'] == 'strong_short':
                score -= 15
            elif analysis_results['trend_consistency'] == 'short':
                score -= 7
            # Добавляем баллы за согласованный импульс
            if analysis_results['momentum_alignment'] == 'aligned_long':
                score += 10
            elif analysis_results['momentum_alignment'] == 'aligned_short':
                score -= 10
            elif analysis_results['momentum_alignment'] == 'divergent':
                score -= 5 # Штраф за расхождение
            # Учитываем волатильность (без изменения счета, но можно использовать позже)
            # Например, очень высокая волатильность может снижать уверенность,
            # а очень низкая может указывать на отсутствие движения.
            # Ограничиваем счет между 0 и 100
            analysis_results['timeframe_agreement_score'] = max(0, min(100, int(score)))
        except Exception as e:
            logger.error(f"Ошибка в calculate_multitimeframe_analysis: {e}")
        return analysis_results

    def calculate_dynamic_levels(self, symbol, data_dict, signal_type):
        """
        Расчет динамических TP/SL уровней на основе ATR и контекста рынка
        """
        if not data_dict or '1h' not in data_dict:
            logger.debug(f"[{symbol}] calculate_dynamic_levels: Нет данных 1h")
            return None
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 20:
            logger.debug(f"[{symbol}] calculate_dynamic_levels: Недостаточно данных")
            return None

        try:
            current_price = float(df_1h['close'].iloc[-1])
            atr = self.get_effective_atr(df_1h, current_price, self.risk_params.get('atr_fallback_pct', 0.01))

            # Индикаторы контекста
            rsi = float(df_1h['rsi'].iloc[-1]) if pd.notna(df_1h['rsi'].iloc[-1]) else 50
            bb_position = float(df_1h['bb_position'].iloc[-1]) if pd.notna(df_1h['bb_position'].iloc[-1]) else 0.5
            momentum_1h = float(df_1h['roc_7'].iloc[-1]) if pd.notna(df_1h['roc_7'].iloc[-1]) else 0.0
            volume_ratio = float(df_1h['volume_ratio'].iloc[-1]) if pd.notna(df_1h['volume_ratio'].iloc[-1]) else 1.0

            # Тренд 1h и 4h
            trend_strength_1h, trend_strength_4h = 0.0, 0.0
            if len(df_1h) > 20:
                trend_strength_1h = (df_1h['close'].iloc[-1] - df_1h['close'].iloc[-20]) / (df_1h['close'].iloc[-20] + 1e-12)
            if '4h' in data_dict and data_dict['4h'] is not None and len(data_dict['4h']) > 20:
                df_4h = data_dict['4h']
                trend_strength_4h = (df_4h['close'].iloc[-1] - df_4h['close'].iloc[-20]) / (df_4h['close'].iloc[-20] + 1e-12)

            # Контекстные множители
            if signal_type.upper() == 'LONG':
                rsi_factor = max(0.7, min(2.0, (70 - rsi) / 20))
                bb_factor = max(0.7, min(2.0, (1.0 - bb_position) * 2.0))
                momentum_factor = max(0.7, min(2.0, 1.0 + momentum_1h * 15.0))
                trend_factor = max(0.7, min(2.0, 1.0 + (trend_strength_1h + trend_strength_4h) * 8.0))
            else:
                rsi_factor = max(0.7, min(2.0, (rsi - 30) / 20))
                bb_factor = max(0.7, min(2.0, bb_position * 2.0))
                momentum_factor = max(0.7, min(2.0, 1.0 - momentum_1h * 15.0))
                trend_factor = max(0.7, min(2.0, 1.0 - (trend_strength_1h + trend_strength_4h) * 8.0))

            volume_factor = max(0.8, min(1.5, volume_ratio * 0.4 + 0.7))
            potential_multiplier = max(0.7, min(2.5, (rsi_factor + bb_factor + momentum_factor + trend_factor + volume_factor) / 5.0))

            # Множители из risk_params
            sl_mult = float(self.risk_params.get('sl_mult', 1.5))
            sl_scale = float(self.risk_params.get('sl_scale', 0.9))
            tp1_mult = float(self.risk_params.get('tp1_mult', 2.0))
            tp2_mult = float(self.risk_params.get('tp2_mult', 3.5))
            tp3_mult = float(self.risk_params.get('tp3_mult', 5.5))

            # Расчёт уровней
            base_sl = atr * sl_mult * sl_scale
            base_tp1 = atr * tp1_mult * potential_multiplier
            base_tp2 = atr * tp2_mult * potential_multiplier
            base_tp3 = atr * tp3_mult * potential_multiplier

            if signal_type.upper() == 'LONG':
                sl  = current_price - base_sl
                tp1 = current_price + base_tp1
                tp2 = current_price + base_tp2
                tp3 = current_price + base_tp3
            else:
                sl  = current_price + base_sl
                tp1 = current_price - base_tp1
                tp2 = current_price - base_tp2
                tp3 = current_price - base_tp3

            # Минимальная дистанция
            min_dist = current_price * float(self.risk_params.get('min_level_dist_pct', 0.001))
            if signal_type.upper() == 'LONG':
                sl  = min(sl, current_price - min_dist)
                tp1 = max(tp1, current_price + min_dist)
                tp2 = max(tp2, tp1 + min_dist)
                tp3 = max(tp3, tp2 + min_dist)
            else:
                sl  = max(sl, current_price + min_dist)
                tp1 = min(tp1, current_price - min_dist)
                tp2 = min(tp2, tp1 - min_dist)
                tp3 = min(tp3, tp2 - min_dist)

            # Округление
            sl  = self.round_price(symbol, sl)
            tp1 = self.round_price(symbol, tp1)
            tp2 = self.round_price(symbol, tp2)
            tp3 = self.round_price(symbol, tp3)

            logger.debug(f"[{symbol}] TP/SL: SL={sl:.8f}, TP1={tp1:.8f}, TP2={tp2:.8f}, TP3={tp3:.8f}")
            return float(sl), float(tp1), float(tp2), float(tp3)

        except Exception as e:
            logger.error(f"Ошибка расчета динамических уровней: {e}")
            return None

        
    def calculate_basic_levels(self, symbol, data_dict, signal_type):
        """
        Базовые TP/SL уровни на случай недоступности динамических данных.
        Учитывает fallback ATR, настройки risk_params и округление по бирже.
        """
        try:
            # --- Получение актуальной цены ---
            if not data_dict or '1h' not in data_dict or data_dict['1h'] is None or len(data_dict['1h']) == 0:
                logger.warning(f"[{symbol}] calculate_basic_levels: Нет данных 1h. Используется fallback цена.")
                current_price = 1000.0
                df_1h = None
            else:
                df_1h = data_dict['1h']
                current_price = float(df_1h['close'].iloc[-1])

            # --- Получение ATR ---
            atr = self.get_effective_atr(df_1h, current_price, self.risk_params.get('atr_fallback_pct', 0.01))
            logger.debug(f"[{symbol}] calculate_basic_levels: Цена={current_price:.8f}, ATR={atr:.8f}")

            # --- Извлечение параметров из risk_params ---
            sl_mult = float(self.risk_params.get('sl_mult', 1.5))
            sl_scale = float(self.risk_params.get('sl_scale', 0.9))
            tp1_mult = float(self.risk_params.get('tp1_mult', 2.0))
            tp2_mult = float(self.risk_params.get('tp2_mult', 3.5))
            tp3_mult = float(self.risk_params.get('tp3_mult', 5.5))
            min_dist_pct = float(self.risk_params.get('min_level_dist_pct', 0.001))
            min_dist = current_price * min_dist_pct

            # --- Расчёт базовых расстояний ---
            sl_distance  = atr * sl_mult * sl_scale
            tp1_distance = atr * tp1_mult
            tp2_distance = atr * tp2_mult
            tp3_distance = atr * tp3_mult

            # --- Построение уровней по направлению ---
            if signal_type.upper() == 'LONG':
                sl  = current_price - sl_distance
                tp1 = current_price + tp1_distance
                tp2 = current_price + tp2_distance
                tp3 = current_price + tp3_distance

                sl  = min(sl, current_price - min_dist)
                tp1 = max(tp1, current_price + min_dist)
                tp2 = max(tp2, tp1 + min_dist)
                tp3 = max(tp3, tp2 + min_dist)

            else:  # SHORT
                sl  = current_price + sl_distance
                tp1 = current_price - tp1_distance
                tp2 = current_price - tp2_distance
                tp3 = current_price - tp3_distance

                sl  = max(sl, current_price + min_dist)
                tp1 = min(tp1, current_price - min_dist)
                tp2 = min(tp2, tp1 - min_dist)
                tp3 = min(tp3, tp2 - min_dist)

            # --- Округление по шагу цены ---
            sl  = self.round_price(symbol, sl)
            tp1 = self.round_price(symbol, tp1)
            tp2 = self.round_price(symbol, tp2)
            tp3 = self.round_price(symbol, tp3)

            logger.debug(f"[{symbol}] calculate_basic_levels: SL={sl:.8f}, TP1={tp1:.8f}, TP2={tp2:.8f}, TP3={tp3:.8f}")
            return float(sl), float(tp1), float(tp2), float(tp3)

        except Exception as e:
            logger.error(f"Ошибка базовых уровней для {symbol}: {e}")
            # --- Жесткий fallback ---
            current_price = 1000.0
            if signal_type.upper() == 'LONG':
                return 990.0, 1005.0, 1015.0, 1025.0
            else:
                return 1010.0, 995.0, 985.0, 975.0


        
    def has_trend_reversed(self, symbol, trade):
        """
        Возвращает (bool, reason) — произошёл ли разворот тренда против сделки.
        Логика: на выбранном ТФ последние N баров подтверждают противоположный тренд:
        - EMA21 против EMA50
        - price_trend_20 против направления сделки с порогом
        - (опц.) MACD-гистограмма подтверждает
        - (опц.) RSI подтверждает
        """
        try:
            if not bool(self.risk_params.get('trend_guard', True)):
                return (False, "")

            # Минимальный срок "созревания" сделки перед проверкой
            min_minutes = int(self.risk_params.get('trend_guard_min_minutes', 15))
            try:
                entry_time = pd.to_datetime(trade.get('timestamp'))
                if entry_time is not None and (datetime.now() - entry_time).total_seconds() < min_minutes * 60:
                    return (False, "")
            except Exception:
                pass

            tf = str(self.risk_params.get('trend_guard_tf', '1h'))
            confirm_bars = int(self.risk_params.get('trend_guard_confirm_bars', 2))
            slope_thr = float(self.risk_params.get('trend_guard_slope_thr', 0.005))
            use_macd = bool(self.risk_params.get('trend_guard_use_macd', True))
            use_rsi = bool(self.risk_params.get('trend_guard_use_rsi', False))

            data_dict = self.fetch_ohlcv_multitimeframe(symbol)
            if not data_dict or tf not in data_dict or data_dict[tf] is None:
                return (False, "")
            df = data_dict[tf]
            df = self.calculate_advanced_indicators(df, tf)
            if df is None or len(df) < max(50, confirm_bars + 25):
                return (False, "")

            # Берём хвост данных
            tail = df.tail(max(confirm_bars, 2)).copy()

            # Гарантия нужных колонок
            needed = ['ema_21', 'ema_50', 'price_trend_20']
            for col in needed:
                if col not in tail.columns:
                    return (False, "")

            direction = trade.get('signal_type', 'LONG').upper()

            def bar_confirms_reversal(row):
                ema_flip_long = row['ema_21'] < row['ema_50']     # против LONG
                ema_flip_short = row['ema_21'] > row['ema_50']    # против SHORT
                slope_down = row['price_trend_20'] < -slope_thr
                slope_up = row['price_trend_20'] > slope_thr

                conds = []
                if direction == 'LONG':
                    conds.append(ema_flip_long)
                    conds.append(slope_down)
                else:  # SHORT
                    conds.append(ema_flip_short)
                    conds.append(slope_up)

                if use_macd and 'macd_histogram' in tail.columns:
                    macd_ok = row['macd_histogram'] < 0 if direction == 'LONG' else row['macd_histogram'] > 0
                    conds.append(macd_ok)

                if use_rsi and 'rsi' in tail.columns:
                    rsi_ok = row['rsi'] < 45 if direction == 'LONG' else row['rsi'] > 55
                    conds.append(rsi_ok)

                # Минимум два ключевых подтверждения
                return sum(1 for c in conds if bool(c)) >= 2

            confirmations = sum(1 for _, r in tail.iterrows() if bar_confirms_reversal(r))
            if confirmations >= confirm_bars:
                reason = (f"trend_guard: tf={tf}, confirm_bars={confirm_bars}, "
                        f"slope_thr={slope_thr}, confirmations={confirmations}/{confirm_bars}")
                return (True, reason)
            return (False, "")
        except Exception as e:
            logger.error(f"Ошибка has_trend_reversed({symbol}): {e}")
            return (False, "")

    # Улучшенная версия метода generate_signal
    def generate_signal(self, symbol, data_dict, multitimeframe_analysis=None):
        """Генерация торгового сигнала с динамическими уровнями (с финальным гейтом после всех корректировок и использованием risk_params)"""
        if not data_dict or '1h' not in data_dict:
            logger.debug(f"❌ {symbol}: Нет данных 1h")
            return None
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 20:
            logger.debug(f"❌ {symbol}: Недостаточно данных ({len(df_1h) if df_1h is not None else 0})")
            return None

        try:
            # --- Настройки рисков из risk_params ---
            min_conf = float(self.risk_params.get('min_confidence_threshold', 60))
            min_rr   = float(self.risk_params.get('min_rr_ratio', 1.5))
            min_vol  = float(self.risk_params.get('min_volume_filter', 250000))
            min_strength = int(self.risk_params.get('min_signal_strength', 3))  # По умолчанию 4
            allow_shorts = bool(self.risk_params.get('use_short_signals', True))

            # --- Фильтр по объёму (1h) ---
            try:
                last_vol = float(df_1h['volume'].iloc[-1])
                if last_vol < min_vol:
                    logger.debug(f"⛔ [{symbol}] Объём {last_vol:.0f} < мин. {min_vol:.0f} — сигнал отклонён")
                    return None
            except Exception:
                # Если объёма нет или NaN — лучше не торговать
                logger.debug(f"⛔ [{symbol}] Не удалось прочитать объём 1h — сигнал отклонён")
                return None

            # --- Базовые метрики 1h ---
            current_price = float(df_1h['close'].iloc[-1])
            atr = float(df_1h['atr'].iloc[-1]) if 'atr' in df_1h.columns and not pd.isna(df_1h['atr'].iloc[-1]) else current_price * 0.02

            rsi = float(df_1h['rsi'].iloc[-1]) if not pd.isna(df_1h['rsi'].iloc[-1]) else 50
            stoch_k = float(df_1h['stoch_k'].iloc[-1]) if not pd.isna(df_1h['stoch_k'].iloc[-1]) else 50
            stoch_d = float(df_1h['stoch_d'].iloc[-1]) if not pd.isna(df_1h['stoch_d'].iloc[-1]) else 50
            macd = float(df_1h['macd'].iloc[-1]) if not pd.isna(df_1h['macd'].iloc[-1]) else 0
            macd_signal = float(df_1h['macd_signal'].iloc[-1]) if not pd.isna(df_1h['macd_signal'].iloc[-1]) else 0
            bb_position = float(df_1h['bb_position'].iloc[-1]) if not pd.isna(df_1h['bb_position'].iloc[-1]) else 0.5
            volume_ratio = float(df_1h['volume_ratio'].iloc[-1]) if not pd.isna(df_1h['volume_ratio'].iloc[-1]) else 1
            momentum_1h = float(df_1h['roc_3'].iloc[-1]) if not pd.isna(df_1h['roc_3'].iloc[-1]) else 0

            # --- Импульс младших ТФ ---
            momentum_5m, momentum_15m = 0.0, 0.0
            if '5m' in data_dict and data_dict['5m'] is not None and len(data_dict['5m']) > 5:
                df_5m = data_dict['5m']
                val = df_5m['roc_3'].iloc[-1]
                momentum_5m = float(val) if not pd.isna(val) else 0.0
            if '15m' in data_dict and data_dict['15m'] is not None and len(data_dict['15m']) > 5:
                df_15m = data_dict['15m']
                val = df_15m['roc_3'].iloc[-1]
                momentum_15m = float(val) if not pd.isna(val) else 0.0

            # --- Тренды 1h/4h ---
            trend_1h, trend_4h = 0.0, 0.0
            if '1h' in data_dict and data_dict['1h'] is not None and len(data_dict['1h']) > 20:
                df_1h_trend = data_dict['1h']
                tr = (df_1h_trend['close'].iloc[-1] - df_1h_trend['close'].iloc[-20]) / df_1h_trend['close'].iloc[-20]
                trend_1h = float(tr) if not pd.isna(tr) else 0.0
            if '4h' in data_dict and data_dict['4h'] is not None and len(data_dict['4h']) > 20:
                df_4h_trend = data_dict['4h']
                tr = (df_4h_trend['close'].iloc[-1] - df_4h_trend['close'].iloc[-20]) / df_4h_trend['close'].iloc[-20]
                trend_4h = float(tr) if not pd.isna(tr) else 0.0

            # --- Условия LONG/SHORT ---
            long_conditions = [
                rsi < 35,
                stoch_k < 30 and stoch_d < 30,
                macd > macd_signal,
                bb_position < 0.2,
                volume_ratio > 1.1,
                momentum_1h > -0.01,
                trend_4h > 0.005,
                trend_1h > -0.01,
            ]
            short_conditions = [
                rsi > 65,
                stoch_k > 70 and stoch_d > 70,
                macd < macd_signal,
                bb_position > 0.8,
                volume_ratio > 1.1,
                momentum_1h < 0.01,
                trend_4h < -0.005,
                trend_1h < 0.01,
            ]

            long_score = sum(1 for cond in long_conditions if cond)
            short_score = sum(1 for cond in short_conditions if cond)

            # --- Выбор направления ---
            signal_type = None
            selected_score = 0
            selected_total = 8  # количество условий в списках

            if long_score >= 4 and short_score >= 4:
                if long_score > short_score:
                    signal_type, selected_score = 'LONG', long_score
                elif short_score > long_score and allow_shorts:
                    signal_type, selected_score = 'SHORT', short_score
                else:
                    signal_type, selected_score = 'LONG', long_score  # равенство -> LONG по умолчанию
            elif long_score >= 4:
                signal_type, selected_score = 'LONG', long_score
            elif short_score >= 4 and allow_shorts:
                signal_type, selected_score = 'SHORT', short_score

            if not signal_type:
                return None

            # --- Базовые метрики уверенности/силы ---
            confidence_score = (selected_score / selected_total) * 100.0
            if selected_score >= 7:
                signal_strength = 5
            elif selected_score >= 6:
                signal_strength = 4
            elif selected_score >= 5:
                signal_strength = 3
            else:
                signal_strength = 2  # при 4 из 8

            # --- Межтаймфреймовый анализ: сначала вето, затем корректировки ---
            if multitimeframe_analysis:
                mt_score = multitimeframe_analysis.get('timeframe_agreement_score', 50)
                mt_trend_consistency = multitimeframe_analysis.get('trend_consistency', 'neutral')
                mt_momentum_alignment = multitimeframe_analysis.get('momentum_alignment', 'neutral')

                # Жёсткое вето
                if mt_score < 30 or mt_momentum_alignment == 'divergent':
                    logger.debug(f"⛔ [{symbol}] Вето по межТФ анализу: score={mt_score}, momentum={mt_momentum_alignment}")
                    return None

                # Коррекции уверенности и силы
                if mt_score >= 70:
                    confidence_score *= 1.10
                elif mt_score <= 30:
                    confidence_score *= 0.90

                if signal_type == 'LONG' and mt_trend_consistency in ['strong_long', 'long']:
                    signal_strength = min(5, signal_strength + 1)
                elif signal_type == 'SHORT' and mt_trend_consistency in ['strong_short', 'short']:
                    signal_strength = min(5, signal_strength + 1)
                elif signal_type == 'LONG' and mt_trend_consistency in ['strong_short', 'short']:
                    signal_strength = max(1, signal_strength - 1)
                elif signal_type == 'SHORT' and mt_trend_consistency in ['strong_long', 'long']:
                    signal_strength = max(1, signal_strength - 1)

            # --- Свечные паттерны: штраф при отсутствии подтверждения ---
            patterns = self.detect_advanced_candlestick_patterns(df_1h)
            if signal_type == 'LONG' and not any(
                p in patterns for p in ['bullish_engulfing', 'hammer', 'morning_star', 'three_white_soldiers', 'bullish_harami', 'tweezer_bottoms']
            ):
                confidence_score *= 0.8
            elif signal_type == 'SHORT' and not any(
                p in patterns for p in ['bearish_engulfing', 'shooting_star', 'evening_star', 'three_black_crows', 'bearish_harami', 'tweezer_tops']
            ):
                confidence_score *= 0.8
            # --- Понижаем confidence, если паттерн против направления ---
            conflicting_patterns = {
                'LONG': ['bearish_engulfing', 'evening_star', 'shooting_star', 'three_black_crows', 'bearish_harami', 'tweezer_tops'],
                'SHORT': ['bullish_engulfing', 'morning_star', 'hammer', 'three_white_soldiers', 'bullish_harami', 'tweezer_bottoms']
            }

            for pattern in patterns:
                if pattern in conflicting_patterns.get(signal_type, []):
                    confidence_score *= 0.85  # множитель можно настраивать
                    logger.debug(f"⚠️ [{symbol}] Паттерн {pattern} против {signal_type} — confidence понижен")


            # --- Финальный гейт после всех корректировок ---
            confidence_score = max(0.0, min(100.0, confidence_score))
            if confidence_score < min_conf or signal_strength < min_strength:
                logger.debug(f"⛔ [{symbol}] Гейт: confidence={confidence_score:.1f} (<{min_conf}) или strength={signal_strength} (<{min_strength}) — отклонён")
                return None

            # --- Расчёт уровней (динамические с fallback) ---
            dynamic_levels_result = self.calculate_dynamic_levels(symbol, data_dict, signal_type)
            if dynamic_levels_result is None:
                logger.debug(f"❌ {symbol}: Ошибка расчёта динамических уровней, используем базовые")
                sl, tp1, tp2, tp3 = self.calculate_basic_levels(symbol, data_dict, signal_type)
                if sl is None or tp1 is None or tp2 is None or tp3 is None:
                    logger.debug(f"❌ {symbol}: Ошибка расчёта базовых уровней")
                    return None
            else:
                sl, tp1, tp2, tp3 = dynamic_levels_result

            # --- Валидация направлений и RR ---
            logger.debug(f"🔍 [{symbol}] Уровни: SL={sl:.8f}, TP1={tp1:.8f}, TP2={tp2:.8f}, TP3={tp3:.8f}, Цена={current_price:.8f}")
            risk_reward_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)

            # Направление уровней
            direction_ok = (signal_type == 'LONG' and sl < current_price < tp3) or (signal_type == 'SHORT' and tp3 < current_price < sl)
            if not direction_ok:
                logger.debug(f"⛔ [{symbol}] Направление уровней некорректно для {signal_type}")
                return None

            if risk_reward_ratio < min_rr:
                logger.debug(f"⛔ [{symbol}] RR={risk_reward_ratio:.2f} < мин. {min_rr:.2f} — отклонён")
                return None

            # --- Потенциал для логирования ---
            potential_upside = ((tp3 - current_price) / current_price * 100.0) if signal_type == 'LONG' else ((current_price - tp3) / current_price * 100.0)

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
                'patterns': list(patterns.keys()),
                'timeframe_analysis': {
                    '5m_momentum': round(float(momentum_5m), 6),
                    '15m_momentum': round(float(momentum_15m), 6),
                    '1h_momentum': round(float(momentum_1h), 6),
                    '1h_trend': round(float(trend_1h), 6),
                    '4h_trend': round(float(trend_4h), 6),
                },
                'technical_analysis': {
                    'rsi': round(float(rsi), 2),
                    'stoch_k': round(float(stoch_k), 2),
                    'macd': round(float(macd), 8),
                    'bb_position': round(float(bb_position), 2),
                    'volume_ratio': round(float(volume_ratio), 2),
                    # ВАЖНО: больше не называем это potential_multiplier, чтобы не вводить в заблуждение
                    'tp3_distance_in_atr5': round(float((abs(tp3 - current_price) / (atr * 5)) if atr > 0 else 1), 2),
                },
                'timestamp': datetime.now().isoformat(),
                'conditions_met': {
                    'long_score': int(long_score),
                    'short_score': int(short_score),
                    'total_conditions': selected_total,
                },
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
        
    def get_effective_atr(self, df, price, fallback_pct=0.01):
        """
        Возвращает ATR из df, если он корректен, иначе fallback = price * fallback_pct.
        """
        try:
            if df is not None and 'atr' in df.columns:
                atr_val = df['atr'].iloc[-1]
                if pd.notna(atr_val) and atr_val > 0:
                    return float(atr_val)
        except Exception:
            pass
        return float(price) * float(fallback_pct)


    def round_price(self, symbol, price):
        """
        Округление цены к шагу цены (tickSize), если доступен, иначе по precision.price.
        """
        try:
            market = self.exchange.market(symbol)
            # Пытаемся достать tickSize из info.filters (Binance)
            tick_size = None
            info = market.get('info', {})
            if isinstance(info, dict):
                filters = info.get('filters') or []
                for f in filters:
                    if f.get('filterType') == 'PRICE_FILTER':
                        # Некоторые биржи дают tickSize как строку
                        ts = f.get('tickSize')
                        if ts is not None:
                            tick_size = float(ts)
                        break

            if tick_size and tick_size > 0:
                return float(round(price / tick_size) * tick_size)

            # Фолбэк по precision
            prec = market.get('precision', {}).get('price', 8)
            return float(f"{price:.{int(prec)}f}")
        except Exception:
            return round(float(price), 8)

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
        # --- НАЧАЛО: Изменения для отображения разницы в цене ---
        entry_price = float(trade['entry_price'])
        signal_type = trade['signal_type']
        price_diff_info = ""
        if current_price is not None:
            # Рассчитываем абсолютную разницу
            price_diff_abs = current_price - entry_price
            # Рассчитываем процентную разницу
            if entry_price != 0: # Избегаем деления на ноль
                price_diff_percent = (price_diff_abs / entry_price) * 100
            else:
                price_diff_percent = 0.0
            # Форматируем строку с разницей
            sign = "+" if price_diff_percent >= 0 else ""
            # Округляем абсолютную разницу до нужного количества знаков после запятой
            # Используем тот же формат, что и для цены (8 знаков)
            price_diff_info = f" | {sign}{price_diff_percent:.2f}% ({sign}{price_diff_abs:.8f})"
        # --- КОНЕЦ: Изменения для отображения разницы в цене ---
        # Обновленная строка лога с информацией о разнице
        if current_price is None:
            logger.info(f"👀 [{symbol}] Отслеживаю {signal_type} | Цена: недоступна | Вход: {entry_price} | Статус: {self.get_trade_status(trade)}")
        else:
            logger.info(f"👀 [{symbol}] Отслеживаю {signal_type} | Цена: {current_price} | Вход: {entry_price} | Статус: {self.get_trade_status(trade)}{price_diff_info}")
        # Проверка TP и SL
        tp_levels = [float(trade['tp1']), float(trade['tp2']), float(trade['tp3'])]
        tp_names = ['TP1', 'TP2', 'TP3']
        # status_changed = False # Убираем, так как теперь логируем закрытия
        for i, (tp, tp_name) in enumerate(zip(tp_levels, tp_names)):
            if f'{tp_name.lower()}_reached' not in trade or not trade[f'{tp_name.lower()}_reached']:
                reached = False
                if signal_type == 'LONG' and current_price >= tp:
                    reached = True
                elif signal_type == 'SHORT' and current_price <= tp:
                    reached = True
                if reached:
                    trade[f'{tp_name.lower()}_reached'] = True
                    # status_changed = True # Убираем
                    logger.info(f"🎯 [{symbol}] --- Take Profit {i+1} достигнут @ {tp} ---")

                    # --- НОВОЕ: Запись события TP в лог закрытий ---
                    closure_info = {
                        'symbol': symbol,
                        'signal_type': signal_type,
                        'entry_time': trade.get('timestamp', datetime.now().isoformat()), # Используем время сигнала или текущее
                        'entry_price': entry_price,
                        'exit_time': datetime.now().isoformat(),
                        'exit_price': tp,
                        'level': tp_name,
                        'price_diff': tp - entry_price if signal_type == 'LONG' else entry_price - tp
                    }
                    self.trade_closures_log.append(self.convert_to_serializable(closure_info))
                    self.save_state() # Сохраняем новое событие
                    self.update_trade_closures_csv() # Обновляем CSV
                    logger.debug(f"Добавлена запись о закрытии TP для {symbol}")
                    # --- КОНЕЦ НОВОГО ---

                    if tp_name == 'TP3':
                        logger.info(f"🎉 [{symbol}] СДЕЛКА ЗАКРЫТА по Take Profit 3 @ {tp}")
                        # self.save_state() # Уже сохранено выше
                        # self.update_signals_csv() # Убираем
                        # self.save_signals_log() # Обновляем и JSON - можно оставить, если нужен JSON
                        # --- ИЗМЕНЕНИЯ: Обновляем JSON лог сигналов и CSV закрытий ---
                        self.save_signals_log() # Обновляем JSON лог сигналов
                        # self.update_trade_closures_csv() # Уже вызвано выше
                        # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                        return 'closed' # Сделка закрывается при TP3

        sl = float(trade['sl'])
        sl_reached = False
        if signal_type == 'LONG' and current_price <= sl:
            sl_reached = True
        elif signal_type == 'SHORT' and current_price >= sl:
            sl_reached = True
        if sl_reached:
            trade['sl_reached'] = True
            # status_changed = True # Убираем
            logger.info(f"🛑 [{symbol}] СДЕЛКА ЗАКРЫТА по Stop Loss @ {sl}")

            # --- НОВОЕ: Запись события SL в лог закрытий ---
            closure_info = {
                'symbol': symbol,
                'signal_type': signal_type,
                'entry_time': trade.get('timestamp', datetime.now().isoformat()),
                'entry_price': entry_price,
                'exit_time': datetime.now().isoformat(),
                'exit_price': sl,
                'level': 'SL',
                'price_diff': sl - entry_price if signal_type == 'LONG' else entry_price - sl
            }
            self.trade_closures_log.append(self.convert_to_serializable(closure_info))
            self.save_state()
            self.update_trade_closures_csv() # Обновляем CSV
            logger.debug(f"Добавлена запись о закрытии SL для {symbol}")
            # --- КОНЕЦ НОВОГО ---

            # self.save_state() # Уже сохранено выше
            # self.update_signals_csv() # Убираем
            # --- ИЗМЕНЕНИЯ: Обновляем JSON лог сигналов и CSV закрытий ---
            self.save_signals_log() # Обновляем JSON лог сигналов
            # self.update_trade_closures_csv() # Уже вызвано выше
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            return 'closed' # Сделка закрывается при SL

        # --- Убираем обновление CSV для активных сделок ---
        # if status_changed:
        #      self.update_signals_csv()
        #      self.save_signals_log() # Обновляем и JSON
        # ---
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
        """Отправка торгового сигнала и добавление сделки в активные"""
        if signal is None:
            return
        symbol = signal['symbol']
        # Проверяем, есть ли уже активная сделка по этой паре.
        if symbol in self.active_trades:
             return # Просто выходим из функции
        # --- Формирование и логирование сигнала (без изменений) ---
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
        if signal.get('patterns'):
            logger.info(f"   🔍 Паттерны: {', '.join(signal['patterns'])}")
        # --- Конец формирования и логирования ---
        # --- Добавляем сделку в активные (без изменений) ---
        self.active_trades[signal['symbol']] = self.convert_to_serializable(signal).copy()
        for tp_name in ['tp1', 'tp2', 'tp3']:
            self.active_trades[signal['symbol']][f'{tp_name}_reached'] = False
        self.active_trades[signal['symbol']]['sl_reached'] = False
        self.save_state()
        # --- Обновляем CSV лог при открытии новой сделки ---
        # Вызываем обновление CSV, чтобы новая сделка появилась в файле
        # self.update_signals_csv() # Убираем
        # Также сохраняем полный JSON лог
        self.save_signals_log() 

    # --- УБРАНО: Старый метод update_signals_csv ---
    # def update_signals_csv(self):
    #     """Обновление CSV лога активных сделок с их текущим статусом"""
    #     try:
    #         if not self.active_trades:
    #              # Если активных сделок нет, создаем/перезаписываем пустой CSV с заголовками
    #              df_empty = pd.DataFrame(columns=[
    #                  'symbol', 'signal_type', 'entry_price', 'potential_upside',
    #                  'signal_strength', 'patterns', 'status', 'last_updated'
    #              ])
    #              df_empty.to_csv('signals_log.csv', index=False)
    #              logger.debug("signals_log.csv обновлен: Нет активных сделок, создан пустой файл.")
    #              return
    #         csv_data = []
    #         for symbol, trade in self.active_trades.items():
    #             # Собираем данные для CSV
    #             csv_entry = {
    #                 'symbol': symbol,
    #                 'signal_type': trade.get('signal_type', 'N/A'),
    #                 'entry_price': float(trade.get('entry_price', 0)),
    #                 'potential_upside': float(trade.get('potential_upside', 0)),
    #                 'signal_strength': int(trade.get('signal_strength', 0)),
    #                 'patterns': ', '.join(trade.get('patterns', [])), # Преобразуем список в строку
    #                 'status': self.get_trade_status(trade), # Получаем текущий статус (hit TP1/2/3, SL, active)
    #                 'last_updated': datetime.now().isoformat() # Добавляем время последнего обновления
    #             }
    #             csv_data.append(csv_entry)
    #         df = pd.DataFrame(csv_data)
    #         # Записываем/перезаписываем CSV файл
    #         df.to_csv('signals_log.csv', index=False)
    #         logger.debug(f"signals_log.csv обновлен: {len(csv_data)} активных сделок.")
    #     except Exception as e:
    #         logger.error(f"Ошибка обновления signals_log.csv: {e}")

    def update_trade_closures_csv(self):
        """Обновление CSV лога закрытых сделок в формате лога сделок"""
        try:
            # Открываем файл в режиме перезаписи
            with open('signals_log.csv', 'w', newline='', encoding='utf-8') as csvfile:
                # Если лог закрытий пуст, файл будет пустым
                if not self.trade_closures_log:
                     logger.debug("signals_log.csv обновлен: Нет записей о закрытиях.")
                     return # Выходим, файл уже пустой

                # Записываем все накопленные записи
                for closure in self.trade_closures_log:
                     # Форматируем строку: symbol,signal_type,entry_time,entry_price,exit_time,exit_price,level,price_diff
                     line = (
                         f"{closure['symbol']},"
                         f"{closure['signal_type']},"
                         f"{closure['entry_time']},"
                         f"{closure['entry_price']:.8f}," # Формат цены входа
                         f"{closure['exit_time']},"
                         f"{closure['exit_price']:.8f},"  # Формат цены выхода
                         f"{closure['level']},"
                         f"{'+' if closure['price_diff'] >= 0 else ''}{closure['price_diff']:.8f}\n" # Формат разницы с знаком
                     )
                     csvfile.write(line)

            logger.debug(f"signals_log.csv обновлен: {len(self.trade_closures_log)} записей о закрытиях.")
        except Exception as e:
            logger.error(f"Ошибка обновления signals_log.csv (закрытия): {e}")

    def save_signals_log(self):
        """Сохранение полного лога всех найденных сигналов в JSON"""
        try:
            # Сохраняем полный JSON лог (это не меняется)
            serializable_signals = [self.convert_to_serializable(signal) for signal in self.signals_found]
            serializable_stats = self.convert_to_serializable(self.analysis_stats)
            with open('signals_log.json', 'w', encoding='utf-8') as f:
                json.dump({
                    'signals': serializable_signals,
                    'stats': serializable_stats,
                    'generated_at': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2, default=str)
            # logger.info("signals_log.json сохранен.") # Опционально, можно убрать
            # --- ВАЖНО: CSV теперь обновляется через update_trade_closures_csv ---
            # Вызываем обновление CSV при каждом сохранении лога (например, после цикла анализа или изменения статуса)
            # Это гарантирует, что CSV всегда отражает актуальное состояние активных сделок.
            # self.update_signals_csv() # Лучше вызывать вручную в нужных местах, чем здесь
        except Exception as e:
            logger.error(f"Ошибка сохранения лога сигналов (JSON): {e}")

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
                # Передаем multitimeframe_analysis в generate_signal
                signal = self.generate_signal(symbol, data_dict, multitimeframe_analysis) 
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
        #logger.info(f"📊 Мониторинг {len(self.symbols)} фьючерсных пар на {len(self.timeframes)} таймфреймах")
        #logger.info(f"💾 Состояние сохраняется в: {self.state_file}")
        #logger.info(f"📈 SHORT сигналы: {'ВКЛЮЧЕНЫ' if self.risk_params['use_short_signals'] else 'ВЫКЛЮЧЕНЫ'}")
        #logger.info(f"🎯 TP/SL рассчитываются на основе потенциала роста и индикаторов")
        # Изменено: Цикл анализа каждую минуту вместо 5
        #logger.info(f"🕒 Цикл анализа каждую минуту")
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
