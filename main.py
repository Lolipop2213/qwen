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
        
        # Список фьючерсных пар (без MATIC/USDT)
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
        self.signal_stats = {}  # Статистика по сигналам
        self.backtest_results = {}  # Результаты бэктеста
        self.performance_metrics = {}  # Метрики производительности
        
        # Кэш данных
        self.data_cache = {}
        self.cache_expiry = 300  # 5 минут
        
        # Имя файла для сохранения состояния
        self.state_file = 'bot_state.json'
        self.analytics_file = 'analytics_data.json'  # Файл для аналитики
        self.backtest_file = 'backtest_results.json'  # Файл для бэктеста
        
        # Технические параметры для краткосрочной торговли
        self.risk_params = {
            'min_confidence_threshold': 45,  # Либеральные параметры
            'min_volume_filter': 300000,
            'min_rr_ratio': 1.2,
            'use_short_signals': True
        }
        
        # Коррелированные пары для фильтрации
        self.correlated_pairs = {
            'BTC/USDT': ['ETH/USDT', 'BNB/USDT'],
            'ETH/USDT': ['BTC/USDT', 'BNB/USDT'],
            'SOL/USDT': ['AVAX/USDT'],
            'ADA/USDT': ['DOT/USDT'],
            'DOT/USDT': ['ADA/USDT']
        }
        
        # Сектора активов для диверсификации
        self.asset_sectors = {
            'BTC/USDT': 'Bitcoin',
            'ETH/USDT': 'Ethereum',
            'BNB/USDT': 'Exchange',
            'SOL/USDT': 'Smart Contracts',
            'ADA/USDT': 'Smart Contracts',
            'DOT/USDT': 'Smart Contracts',
            'AVAX/USDT': 'Smart Contracts',
            'LINK/USDT': 'Oracle',
            'UNI/USDT': 'DEX',
            'DOGE/USDT': 'Meme',
            'XRP/USDT': 'Payments',
            'LTC/USDT': 'Payments',
            'ATOM/USDT': 'Interoperability',
            'FIL/USDT': 'Storage',
            'TRX/USDT': 'Entertainment',
            'VET/USDT': 'Supply Chain',
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
        self.load_analytics()  # Загрузка аналитических данных
        
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
            
    def load_analytics(self):
        """Загрузка аналитических данных"""
        try:
            if os.path.exists('analytics_data.json'):
                with open('analytics_data.json', 'r', encoding='utf-8') as f:
                    analytics_data = json.load(f)
                    
                if 'signal_stats' in analytics_data:
                    self.signal_stats = analytics_data['signal_stats']
                    logger.info(f"📊 Загружено аналитики по {len(self.signal_stats)} активам")
                    
                if 'performance_metrics' in analytics_data:
                    self.performance_metrics = analytics_data['performance_metrics']
                    logger.info("📊 Метрики производительности загружены")
                    
                if 'backtest_results' in analytics_data:
                    self.backtest_results = analytics_data['backtest_results']
                    logger.info(f"📊 Загружено {len(self.backtest_results)} результатов бэктеста")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки аналитики: {e}")
            
    def save_analytics(self):
        """Сохранение аналитических данных"""
        try:
            analytics_data = {
                'signal_stats': self.convert_to_serializable(self.signal_stats),
                'performance_metrics': self.convert_to_serializable(self.performance_metrics),
                'backtest_results': self.convert_to_serializable(self.backtest_results),
                'updated_at': datetime.now().isoformat()
            }
            
            with open(self.analytics_file, 'w', encoding='utf-8') as f:
                json.dump(analytics_data, f, ensure_ascii=False, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения аналитики: {e}")
            
    def save_backtest_results(self, symbol, results):
        """Сохранение результатов бэктеста"""
        try:
            self.backtest_results[symbol] = self.convert_to_serializable(results)
            
            # Сохраняем в отдельный файл
            backtest_data = {
                'results': self.convert_to_serializable(self.backtest_results),
                'updated_at': datetime.now().isoformat()
            }
            
            with open(self.backtest_file, 'w', encoding='utf-8') as f:
                json.dump(backtest_data, f, ensure_ascii=False, indent=2, default=str)
                
            logger.info(f"💾 Бэктест для {symbol} сохранен")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения бэктеста для {symbol}: {e}")
            
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
        self.data_cache[cache_key] = (data, current_time)
        return data
            
    def fetch_ohlcv_multitimeframe(self, symbol):
        """Получение фьючерсных данных по нескольким таймфреймам"""
        data = {}
        try:
            for tf in self.timeframes:
                limit = 200 if tf in ['5m', '15m'] else 100
                ohlcv = self.fetch_ohlcv_with_cache(symbol, tf, limit=limit)
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
        
    def robust_data_fetch(self, symbol, timeframes):
        """Надежное получение данных с повторными попытками"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return self.fetch_ohlcv_multitimeframe(symbol)
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1} получения данных для {symbol} не удалась: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    logger.error(f"Не удалось получить данные для {symbol} после {max_retries} попыток")
                    return None
                    
    def get_trend_direction(self, df):
        """Определение направления тренда"""
        if df is None or len(df) < 20:
            return 'neutral'
            
        try:
            current_price = float(df['close'].iloc[-1])
            ema_21 = float(df['ema_21'].iloc[-1]) if 'ema_21' in df.columns else current_price
            return 'bullish' if current_price > ema_21 else 'bearish'
        except:
            return 'neutral'
            
    def check_multitimeframe_confirmation(self, data_dict, signal_type):
        """МЯГКАЯ проверка подтверждения на разных таймфреймах"""
        confirmations = 0
        total_timeframes = 0
        
        for tf, df in data_dict.items():
            if df is not None and len(df) > 20:
                total_timeframes += 1
                trend = self.get_trend_direction(df)
                if (signal_type == 'LONG' and trend == 'bullish') or \
                   (signal_type == 'SHORT' and trend == 'bearish'):
                    confirmations += 1
        
        # ТОЛЬКО 50% согласования
        return confirmations >= max(1, total_timeframes * 0.5)
        
    def check_volatility_filter(self, df_1h):
        """БОЛЕЕ ЛИБЕРАЛЬНЫЙ фильтр волатильности"""
        if df_1h is None or len(df_1h) < 20:
            return True
            
        try:
            atr = float(df_1h['atr'].iloc[-1])
            price = float(df_1h['close'].iloc[-1])
            volatility_percent = (atr / price) * 100
            
            # ШИРЕ диапазон: 0.5-8%
            return 0.5 <= volatility_percent <= 8.0
        except:
            return True
            
    def check_volume_profile(self, data_dict):
        """МЯГКИЙ объемный фильтр"""
        if '1h' not in data_dict or data_dict['1h'] is None:
            return True
            
        df = data_dict['1h']
        if len(df) < 50:
            return True
            
        try:
            current_volume = float(df['volume'].iloc[-1])
            avg_volume_24h = float(df['volume'].tail(24).mean())
            
            # МЕНЬШЕ требование: 30%
            return current_volume >= avg_volume_24h * 0.3
        except:
            return True
            
    def calculate_fibonacci_levels(self, df, signal_type):
        """Расчет уровней Фибоначчи"""
        if df is None or len(df) < 50:
            return {}
            
        try:
            # Находим свинг хай/лоу за последние 30 баров
            recent_data = df.tail(30)
            swing_high = float(recent_data['high'].max())
            swing_low = float(recent_data['low'].min())
            
            diff = swing_high - swing_low
            
            if signal_type == 'LONG':
                fib_levels = {
                    'fib_236': swing_low + diff * 0.236,
                    'fib_382': swing_low + diff * 0.382,
                    'fib_500': swing_low + diff * 0.500,
                    'fib_618': swing_low + diff * 0.618,
                    'fib_786': swing_low + diff * 0.786
                }
            else:
                fib_levels = {
                    'fib_236': swing_high - diff * 0.236,
                    'fib_382': swing_high - diff * 0.382,
                    'fib_500': swing_high - diff * 0.500,
                    'fib_618': swing_high - diff * 0.618,
                    'fib_786': swing_high - diff * 0.786
                }
                
            return fib_levels
        except:
            return {}
            
    def calculate_dynamic_rr_ratio(self, symbol, data_dict, signal_type):
        """Динамический расчет RR на основе рыночных условий"""
        if not data_dict or '1h' not in data_dict:
            return 1.5  # Дефолтное значение
            
        df = data_dict['1h']
        if df is None or len(df) < 20:
            return 1.5
            
        try:
            # Анализ волатильности
            volatility = float(df['volatility'].iloc[-1]) if 'volatility' in df.columns else 0.02
            
            # Анализ тренда
            trend_strength = abs(float(df['price_trend_20'].iloc[-1]))
            
            # Базовый RR
            base_rr = 1.5
            
            # Корректировка по волатильности
            if volatility > 0.03:  # Высокая волатильность
                base_rr = 1.3  # Меньше RR для высокой волатильности
            elif volatility < 0.01:  # Низкая волатильность
                base_rr = 1.8  # Больше RR для низкой волатильности
                
            # Корректировка по тренду
            if trend_strength > 0.05:  # Сильный тренд
                base_rr *= 1.2  # Больше RR для сильного тренда
            else:
                base_rr *= 0.9  # Меньше RR для слабого тренда
                
            return max(1.2, min(3.0, base_rr))  # Ограничиваем 1.2 - 3.0
        except:
            return 1.5
            
    def check_correlation_filter(self, symbol, active_trades):
        """Фильтр по корреляции - избегаем одновременных позиций в коррелирующих активах"""
        if not active_trades:
            return True
            
        if symbol in self.correlated_pairs:
            for active_symbol in active_trades:
                if active_symbol in self.correlated_pairs[symbol]:
                    return False  # Уже есть позиция в коррелирующем активе
                    
        return True
        
    def get_asset_sector(self, symbol):
        """Определение сектора актива"""
        return self.asset_sectors.get(symbol, 'Other')

    def check_sector_diversification(self, symbol, active_trades):
        """Проверка диверсификации по секторам"""
        if not active_trades:
            return True
            
        current_sector = self.get_asset_sector(symbol)
        sector_positions = {}
        
        # Подсчет позиций по секторам
        for active_symbol in active_trades:
            sector = self.get_asset_sector(active_symbol)
            sector_positions[sector] = sector_positions.get(sector, 0) + 1
        
        # Ограничение: максимум 30% позиций в одном секторе
        current_sector_count = sector_positions.get(current_sector, 0)
        total_positions = len(active_trades) + 1  # +1 для новой позиции
        
        sector_percentage = (current_sector_count + 1) / total_positions
        return sector_percentage <= 0.3
        
    def calculate_portfolio_risk(self, symbol, entry_price, sl_price, active_trades):
        """Расчет риска по всему портфелю"""
        # Риск новой позиции
        position_risk = abs(entry_price - sl_price) / entry_price
        
        # Общий риск портфеля
        total_portfolio_risk = position_risk
        
        for active_symbol, trade in active_trades.items():
            try:
                active_entry = float(trade['entry_price'])
                active_sl = float(trade['sl'])
                active_risk = abs(active_entry - active_sl) / active_entry
                total_portfolio_risk += active_risk
            except:
                pass
        
        # Максимальный допустимый риск портфеля
        max_portfolio_risk = 0.15  # 15%
        return total_portfolio_risk <= max_portfolio_risk
        
    def detect_advanced_candlestick_patterns(self, df):
        """Обнаружение продвинутых паттернов свечей"""
        if df is None or len(df) < 10:
            return {}
            
        patterns = {}
        
        try:
            # AB=CD паттерн
            if len(df) >= 4:
                # Рассчитываем отношение ног
                leg1 = abs(df['close'].iloc[-4] - df['close'].iloc[-3])  # AB
                leg2 = abs(df['close'].iloc[-2] - df['close'].iloc[-1])   # CD
                
                if 0.9 <= leg1/leg2 <= 1.1:  # Равные ноги ±10%
                    patterns['abcd_pattern'] = True
                    
            # Голова и плечи
            if len(df) >= 5:
                left_shoulder = df['high'].iloc[-5]
                head = df['high'].iloc[-4] 
                right_shoulder = df['high'].iloc[-3]
                neckline = df['low'].iloc[-2:-1].mean()
                
                if (head > left_shoulder and head > right_shoulder and
                    left_shoulder * 0.95 <= right_shoulder <= left_shoulder * 1.05):
                    patterns['head_shoulders'] = True
                    
            # Три белых солдата / три черных ворона
            if len(df) >= 3:
                three_green = all(df['close'].iloc[i] > df['open'].iloc[i] for i in [-3, -2, -1])
                three_red = all(df['close'].iloc[i] < df['open'].iloc[i] for i in [-3, -2, -1])
                
                if three_green:
                    patterns['three_white_soldiers'] = True
                elif three_red:
                    patterns['three_black_crows'] = True
                    
            # Doji и Spinning Top
            body_range = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
            total_range = df['high'].iloc[-1] - df['low'].iloc[-1]
            
            if total_range > 0 and body_range/total_range < 0.1:
                patterns['doji'] = True
            elif 0.1 <= body_range/total_range <= 0.3:
                patterns['spinning_top'] = True
                
        except Exception as e:
            logger.error(f"Ошибка обнаружения паттернов: {e}")
            
        return patterns

    def calculate_multitimeframe_consensus(self, data_dict):
        """Многотаймфреймовый консенсус тренда"""
        if not data_dict:
            return {'consensus_score': 0, 'agreement_level': 'neutral'}
            
        consensus_signals = []
        timeframes_analyzed = 0
        
        # Анализируем каждый таймфрейм
        priority_timeframes = ['1h', '4h', '1d']  # Приоритетные таймфреймы
        
        for tf in priority_timeframes:
            if tf in data_dict and data_dict[tf] is not None and len(data_dict[tf]) >= 20:
                df = data_dict[tf]
                timeframes_analyzed += 1
                
                try:
                    # Трендовые индикаторы
                    current_price = float(df['close'].iloc[-1])
                    ema_21 = float(df['ema_21'].iloc[-1]) if 'ema_21' in df.columns else current_price
                    ema_50 = float(df['ema_50'].iloc[-1]) if 'ema_50' in df.columns else current_price
                    macd = float(df['macd'].iloc[-1]) if 'macd' in df.columns else 0
                    macd_signal = float(df['macd_signal'].iloc[-1]) if 'macd_signal' in df.columns else 0
                    rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df.columns else 50
                    
                    # Оценка силы тренда
                    trend_strength = abs(current_price - ema_21) / ema_21 * 100
                    
                    # Определяем сигнал по каждому таймфрейму
                    if (current_price > ema_21 and current_price > ema_50 and 
                        macd > macd_signal and rsi < 70):
                        signal = 'strong_bullish'
                        strength = min(100, trend_strength * 2)
                    elif (current_price < ema_21 and current_price < ema_50 and 
                          macd < macd_signal and rsi > 30):
                        signal = 'strong_bearish'
                        strength = min(100, trend_strength * 2)
                    elif current_price > ema_21:
                        signal = 'bullish'
                        strength = min(70, trend_strength)
                    elif current_price < ema_21:
                        signal = 'bearish'
                        strength = min(70, trend_strength)
                    else:
                        signal = 'neutral'
                        strength = 30
                        
                    consensus_signals.append({
                        'timeframe': tf,
                        'signal': signal,
                        'strength': strength,
                        'price': current_price
                    })
                    
                except Exception as e:
                    logger.debug(f"Ошибка анализа {tf}: {e}")
                    
        if not consensus_signals:
            return {'consensus_score': 0, 'agreement_level': 'neutral'}
            
        # Рассчитываем консенсус
        total_strength = sum(signal['strength'] for signal in consensus_signals)
        bullish_count = len([s for s in consensus_signals if 'bullish' in s['signal']])
        bearish_count = len([s for s in consensus_signals if 'bearish' in s['signal']])
        
        # Уровень согласия
        agreement_ratio = abs(bullish_count - bearish_count) / len(consensus_signals)
        agreement_level = 'high' if agreement_ratio > 0.6 else 'medium' if agreement_ratio > 0.3 else 'low'
        
        # Средняя сила сигнала
        avg_strength = total_strength / len(consensus_signals) if consensus_signals else 0
        
        # Взвешенный консенсус (более старшие таймфреймы имеют больший вес)
        weighted_score = 0
        total_weight = 0
        
        for signal in consensus_signals:
            weight = 1.0
            if signal['timeframe'] == '1d':
                weight = 3.0
            elif signal['timeframe'] == '4h':
                weight = 2.0
            elif signal['timeframe'] == '1h':
                weight = 1.0
                
            signal_value = 50  # neutral
            if 'bullish' in signal['signal']:
                signal_value = 100 if signal['signal'] == 'strong_bullish' else 75
            elif 'bearish' in signal['signal']:
                signal_value = 0 if signal['signal'] == 'strong_bearish' else 25
                
            weighted_score += signal_value * weight
            total_weight += weight
            
        consensus_score = weighted_score / total_weight if total_weight > 0 else 50
        
        return {
            'consensus_score': round(consensus_score, 2),
            'agreement_level': agreement_level,
            'timeframes_analyzed': timeframes_analyzed,
            'bullish_signals': bullish_count,
            'bearish_signals': bearish_count,
            'avg_strength': round(avg_strength, 2),
            'detailed_signals': consensus_signals
        }

    def calculate_wyckoff_levels(self, df, signal_type):
        """Расчет уровней по методу Вайкоффа"""
        if df is None or len(df) < 50:
            return {}
            
        try:
            # Анализ объемного профиля за последние 50 баров
            recent_data = df.tail(50)
            
            # Определяем фазы Вайкоффа
            volume_profile = {}
            price_levels = {}
            
            # Группируем цены по уровням (с точностью до 0.5% от цены)
            current_price = float(recent_data['close'].iloc[-1])
            price_step = current_price * 0.005  # 0.5%
            
            for _, row in recent_data.iterrows():
                price = float(row['close'])
                volume = float(row['volume'])
                price_level = round(price / price_step) * price_step
                
                if price_level not in volume_profile:
                    volume_profile[price_level] = 0
                    price_levels[price_level] = []
                    
                volume_profile[price_level] += volume
                price_levels[price_level].append(price)
            
            # Находим значимые уровни поддержки/сопротивления
            significant_levels = []
            avg_volume = np.mean(list(volume_profile.values()))
            
            for price_level, volume in volume_profile.items():
                if volume > avg_volume * 1.5:  # Значимый объем (на 50% выше среднего)
                    price_range = price_levels[price_level]
                    level_info = {
                        'price': price_level,
                        'volume': volume,
                        'count': len(price_range),
                        'avg_price': np.mean(price_range),
                        'volatility': np.std(price_range) if len(price_range) > 1 else 0
                    }
                    significant_levels.append(level_info)
            
            # Сортируем по объему
            significant_levels.sort(key=lambda x: x['volume'], reverse=True)
            
            # Определяем потенциальные TP/SL
            wyckoff_levels = {}
            current_price = float(df['close'].iloc[-1])
            
            if signal_type == 'LONG':
                # Находим уровни сопротивления выше текущей цены
                resistance_levels = [level for level in significant_levels if level['price'] > current_price]
                resistance_levels.sort(key=lambda x: x['price'])
                
                if resistance_levels:
                    wyckoff_levels['tp1'] = resistance_levels[0]['price'] if len(resistance_levels) > 0 else current_price * 1.02
                    wyckoff_levels['tp2'] = resistance_levels[1]['price'] if len(resistance_levels) > 1 else current_price * 1.04
                    wyckoff_levels['tp3'] = resistance_levels[2]['price'] if len(resistance_levels) > 2 else current_price * 1.06
                    wyckoff_levels['sl'] = current_price - (wyckoff_levels['tp3'] - current_price) * 0.5
                    
            else:  # SHORT
                # Находим уровни поддержки ниже текущей цены
                support_levels = [level for level in significant_levels if level['price'] < current_price]
                support_levels.sort(key=lambda x: x['price'], reverse=True)
                
                if support_levels:
                    wyckoff_levels['tp1'] = support_levels[0]['price'] if len(support_levels) > 0 else current_price * 0.98
                    wyckoff_levels['tp2'] = support_levels[1]['price'] if len(support_levels) > 1 else current_price * 0.96
                    wyckoff_levels['tp3'] = support_levels[2]['price'] if len(support_levels) > 2 else current_price * 0.94
                    wyckoff_levels['sl'] = current_price + (current_price - wyckoff_levels['tp3']) * 0.5
                    
            # Добавляем дополнительную информацию
            wyckoff_levels['significant_volume_levels'] = len(significant_levels)
            wyckoff_levels['avg_volume_multiple'] = avg_volume / float(df['volume'].tail(20).mean()) if len(df) >= 20 else 1
            
            return wyckoff_levels
            
        except Exception as e:
            logger.error(f"Ошибка расчета уровней Вайкоффа: {e}")
            return {}

    def calculate_dynamic_fibonacci_levels(self, df, signal_type):
        """Динамические уровни Фибоначчи с адаптацией"""
        if df is None or len(df) < 100:
            return {}
            
        try:
            # Находим значимые свинг хай/лоу за последние 100 баров
            recent_data = df.tail(100)
            
            # Определяем локальные экстремумы
            swing_highs = []
            swing_lows = []
            
            for i in range(20, len(recent_data) - 20):
                current_high = float(recent_data['high'].iloc[i])
                current_low = float(recent_data['low'].iloc[i])
                
                # Проверяем, является ли текущий бар свинг хай
                is_swing_high = True
                for j in range(i-20, i+21):
                    if j >= 0 and j < len(recent_data):
                        if float(recent_data['high'].iloc[j]) > current_high:
                            is_swing_high = False
                            break
                            
                if is_swing_high:
                    swing_highs.append({
                        'price': current_high,
                        'timestamp': recent_data.index[i],
                        'volume': float(recent_data['volume'].iloc[i])
                    })
                
                # Проверяем, является ли текущий бар свинг лоу
                is_swing_low = True
                for j in range(i-20, i+21):
                    if j >= 0 and j < len(recent_data):
                        if float(recent_data['low'].iloc[j]) < current_low:
                            is_swing_low = False
                            break
                            
                if is_swing_low:
                    swing_lows.append({
                        'price': current_low,
                        'timestamp': recent_data.index[i],
                        'volume': float(recent_data['volume'].iloc[i])
                    })
            
            # Сортируем по объему (более значимые уровни)
            swing_highs.sort(key=lambda x: x['volume'], reverse=True)
            swing_lows.sort(key=lambda x: x['volume'], reverse=True)
            
            # Берем топ-3 уровней
            top_highs = swing_highs[:3]
            top_lows = swing_lows[:3]
            
            fibonacci_levels = {}
            current_price = float(df['close'].iloc[-1])
            
            if signal_type == 'LONG' and top_lows:
                # Используем ближайший свинг лоу как базу
                base_level = min(top_lows, key=lambda x: abs(x['price'] - current_price))['price']
                diff = current_price - base_level
                
                # Адаптивные коэффициенты Фибоначчи
                fib_ratios = [0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618]
                
                fibonacci_levels = {
                    f'fib_{int(ratio*1000)}': current_price + diff * ratio
                    for ratio in fib_ratios
                }
                
            elif signal_type == 'SHORT' and top_highs:
                # Используем ближайший свинг хай как базу
                base_level = min(top_highs, key=lambda x: abs(x['price'] - current_price))['price']
                diff = base_level - current_price
                
                # Адаптивные коэффициенты Фибоначчи
                fib_ratios = [0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618]
                
                fibonacci_levels = {
                    f'fib_{int(ratio*1000)}': current_price - diff * ratio
                    for ratio in fib_ratios
                }
                
            # Добавляем TP1-3 и SL
            if fibonacci_levels:
                sorted_levels = sorted(fibonacci_levels.values())
                
                if signal_type == 'LONG':
                    fibonacci_levels['tp1'] = sorted_levels[min(2, len(sorted_levels)-1)]  # 38.2%
                    fibonacci_levels['tp2'] = sorted_levels[min(4, len(sorted_levels)-1)]  # 61.8%
                    fibonacci_levels['tp3'] = sorted_levels[min(6, len(sorted_levels)-1)]  # 100%
                    fibonacci_levels['sl'] = current_price - (fibonacci_levels['tp3'] - current_price) * 0.5
                    
                else:  # SHORT
                    fibonacci_levels['tp1'] = sorted_levels[max(0, len(sorted_levels)-3)]  # 38.2%
                    fibonacci_levels['tp2'] = sorted_levels[max(0, len(sorted_levels)-5)]  # 61.8%
                    fibonacci_levels['tp3'] = sorted_levels[max(0, len(sorted_levels)-7)]  # 100%
                    fibonacci_levels['sl'] = current_price + (current_price - fibonacci_levels['tp3']) * 0.5
                    
            return fibonacci_levels
            
        except Exception as e:
            logger.error(f"Ошибка расчета динамических уровней Фибоначчи: {e}")
            return {}

    def calculate_market_cycle_analysis(self, data_dict):
        """Анализ рыночного цикла"""
        if not data_dict or '1h' not in data_dict:
            return {'cycle_phase': 'unknown', 'cycle_confidence': 0}
            
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 100:
            return {'cycle_phase': 'unknown', 'cycle_confidence': 0}
            
        try:
            # Анализ рыночного цикла на основе:
            # 1. Волатильность
            # 2. Объем
            # 3. Трендовая сила
            # 4. RSI дивергенции
            
            recent_data = df_1h.tail(50)
            
            # Волатильность (ATR)
            atr_values = recent_data['atr'].dropna().values if 'atr' in recent_data.columns else []
            avg_atr = np.mean(atr_values) if len(atr_values) > 0 else 0.02
            current_price = float(recent_data['close'].iloc[-1])
            volatility_percent = (avg_atr / current_price) * 100
            
            # Объемный анализ
            volume_ratio = float(recent_data['volume_ratio'].iloc[-1]) if 'volume_ratio' in recent_data.columns else 1
            avg_volume_ratio = float(recent_data['volume_ratio'].tail(20).mean()) if 'volume_ratio' in recent_data.columns else 1
            
            # Трендовая сила
            trend_20 = float(recent_data['price_trend_20'].iloc[-1]) if 'price_trend_20' in recent_data.columns else 0
            trend_50 = float(recent_data['price_trend_50'].iloc[-1]) if 'price_trend_50' in recent_data.columns else 0
            
            # RSI анализ
            rsi_values = recent_data['rsi'].dropna().values if 'rsi' in recent_data.columns else []
            rsi_trend = np.polyfit(range(len(rsi_values[-10:])), rsi_values[-10:], 1)[0] if len(rsi_values) >= 10 else 0
            
            # Определение фазы цикла
            cycle_phase = 'accumulation'  # По умолчанию накопление
            cycle_confidence = 50  # По умолчанию средняя уверенность
            
            # Анализ для бычьего рынка (накопление → размах → распределение)
            if volatility_percent > 3:  # Высокая волатильность
                if volume_ratio > 1.5 and trend_20 > 0.02:  # Высокий объем + сильный тренд
                    cycle_phase = 'markup'  # Размах
                    cycle_confidence = 80
                elif volume_ratio < 0.8:  # Низкий объем
                    cycle_phase = 'distribution'  # Распределение
                    cycle_confidence = 70
                else:
                    cycle_phase = 'accumulation'  # Накопление
                    cycle_confidence = 60
            elif volatility_percent < 1:  # Низкая волатильность
                if abs(trend_20) < 0.01:  # Плоский тренд
                    cycle_phase = 'consolidation'  # Консолидация
                    cycle_confidence = 75
                else:
                    cycle_phase = 'accumulation'  # Накопление
                    cycle_confidence = 65
            else:  # Средняя волатильность
                if trend_20 > 0.01 and rsi_trend > 0:  # Восходящий тренд + RSI растет
                    cycle_phase = 'markup'  # Размах
                    cycle_confidence = 70
                elif trend_20 < -0.01 and rsi_trend < 0:  # Нисходящий тренд + RSI падает
                    cycle_phase = 'markdown'  # Слив
                    cycle_confidence = 70
                else:
                    cycle_phase = 'accumulation'  # Накопление
                    cycle_confidence = 55
                    
            return {
                'cycle_phase': cycle_phase,
                'cycle_confidence': cycle_confidence,
                'volatility_percent': round(volatility_percent, 2),
                'volume_ratio': round(volume_ratio, 2),
                'trend_20': round(trend_20 * 100, 2),
                'trend_50': round(trend_50 * 100, 2),
                'rsi_trend': round(rsi_trend, 4),
                'avg_volume_ratio': round(avg_volume_ratio, 2)
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа рыночного цикла: {e}")
            return {'cycle_phase': 'unknown', 'cycle_confidence': 0}

    def apply_cycle_based_filtering(self, symbol, signal, market_cycle):
        """Применение фильтрации на основе рыночного цикла"""
        if not signal or not market_cycle:
            return signal
            
        try:
            cycle_phase = market_cycle.get('cycle_phase', 'unknown')
            cycle_confidence = market_cycle.get('cycle_confidence', 50)
            
            # Адаптация параметров в зависимости от фазы цикла
            if cycle_phase == 'markup':  # Размах - лучшее время для LONG
                if signal['signal_type'] == 'LONG':
                    # Увеличиваем потенциал для LONG сигналов
                    signal['confidence'] = min(100, signal['confidence'] * 1.2)
                    signal['risk_reward_ratio'] = signal['risk_reward_ratio'] * 1.1
                elif signal['signal_type'] == 'SHORT':
                    # Снижаем уверенность для SHORT сигналов
                    signal['confidence'] = signal['confidence'] * 0.7
                    
            elif cycle_phase == 'markdown':  # Слив - лучшее время для SHORT
                if signal['signal_type'] == 'SHORT':
                    # Увеличиваем потенциал для SHORT сигналов
                    signal['confidence'] = min(100, signal['confidence'] * 1.2)
                    signal['risk_reward_ratio'] = signal['risk_reward_ratio'] * 1.1
                elif signal['signal_type'] == 'LONG':
                    # Снижаем уверенность для LONG сигналов
                    signal['confidence'] = signal['confidence'] * 0.7
                    
            elif cycle_phase == 'distribution':  # Распределение - осторожнее
                # Снижаем уверенность для всех сигналов
                signal['confidence'] = signal['confidence'] * 0.8
                signal['risk_reward_ratio'] = signal['risk_reward_ratio'] * 0.9
                
            elif cycle_phase == 'consolidation':  # Консолидация - очень осторожно
                # Сильно снижаем уверенность
                signal['confidence'] = signal['confidence'] * 0.6
                signal['risk_reward_ratio'] = signal['risk_reward_ratio'] * 0.7
                # Увеличиваем требования к RR
                if signal['risk_reward_ratio'] < 1.5:
                    return None  # Слишком низкий RR в консолидации
                    
            # Проверка уверенности на основе цикла
            min_confidence = self.risk_params['min_confidence_threshold']
            if cycle_confidence < 60:  # Низкая уверенность в цикле
                min_confidence += 10  # Ужесточаем требования
                
            if signal['confidence'] < min_confidence:
                return None
                
            return signal
            
        except Exception as e:
            logger.error(f"Ошибка применения фильтрации по циклу для {symbol}: {e}")
            return signal

    def calculate_index_correlation_filter(self, symbol, data_dict):
        """Фильтр по корреляции с рыночными индексами"""
        if not data_dict or '1h' not in data_dict:
            return True  # Пропускаем фильтр если нет данных
            
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 50:
            return True
            
        try:
            # Получаем данные по BTC (рыночный индекс)
            btc_data = self.fetch_ohlcv_with_cache('BTC/USDT', '1h', limit=50)
            if not btc_data or len(btc_data) < 50:
                return True
                
            df_btc = pd.DataFrame(btc_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_btc['timestamp'] = pd.to_datetime(df_btc['timestamp'], unit='ms')
            df_btc.set_index('timestamp', inplace=True)
            
            # Синхронизируем данные
            common_index = df_1h.index.intersection(df_btc.index)
            if len(common_index) < 30:  # Мало общих данных
                return True
                
            df_sync_1h = df_1h.loc[common_index]
            df_sync_btc = df_btc.loc[common_index]
            
            # Рассчитываем доходности
            returns_1h = df_sync_1h['close'].pct_change().dropna()
            returns_btc = df_sync_btc['close'].pct_change().dropna()
            
            # Корреляция с BTC
            if len(returns_1h) > 10 and len(returns_btc) > 10:
                min_len = min(len(returns_1h), len(returns_btc))
                correlation_with_btc = np.corrcoef(
                    returns_1h.tail(min_len), 
                    returns_btc.tail(min_len)
                )[0, 1]
                
                # Для альткоинов: хотим положительную корреляцию в бычьем рынке
                # Для хеджирования: хотим отрицательную корреляцию
                
                asset_type = self.get_asset_sector(symbol)
                
                if asset_type in ['Bitcoin', 'Ethereum']:  # BTC/ETH
                    # BTC/ETH должны двигаться независимо от других
                    if abs(correlation_with_btc) > 0.8:  # Слишком высокая автокорреляция
                        logger.debug(f"❌ {symbol}: Слишком высокая корреляция с BTC ({correlation_with_btc:.2f})")
                        return False
                        
                elif asset_type in ['Smart Contracts', 'DeFi']:  # Альткоины
                    # Альткоины обычно коррелируют с BTC > 0.5
                    if correlation_with_btc < 0.3:
                        logger.debug(f"❌ {symbol}: Низкая корреляция с BTC ({correlation_with_btc:.2f})")
                        return False
                        
                else:  # Другие активы
                    # Проверяем нормальный уровень корреляции
                    if abs(correlation_with_btc) > 0.9:  # Слишком высокая
                        logger.debug(f"❌ {symbol}: Аномальная корреляция с BTC ({correlation_with_btc:.2f})")
                        return False
                        
            return True
            
        except Exception as e:
            logger.error(f"Ошибка фильтрации по корреляции для {symbol}: {e}")
            return True  # Пропускаем фильтр при ошибке

    def adaptive_parameter_optimization(self, symbol):
        """Адаптивная оптимизация параметров на основе истории"""
        if symbol not in self.signal_stats:
            return self.risk_params  # Возвращаем дефолтные параметры
            
        stats = self.signal_stats[symbol]
        
        # Анализируем недавние результаты
        recent_results = stats.get('recent_results', [])
        if len(recent_results) < 10:
            return self.risk_params  # Недостаточно данных
            
        recent_win_rate = sum(recent_results[-10:]) / 10
        overall_win_rate = stats.get('win_rate', 0.5)
        avg_pnl = stats.get('avg_pnl', 0)
        
        # Адаптируем параметры
        optimized_params = self.risk_params.copy()
        
        # Если недавние результаты плохие (< 40%)
        if recent_win_rate < 0.4:
            # Ужесточаем фильтры
            optimized_params['min_confidence_threshold'] = min(75, optimized_params['min_confidence_threshold'] + 10)
            optimized_params['min_rr_ratio'] = min(2.5, optimized_params['min_rr_ratio'] + 0.3)
            logger.info(f"📉 {symbol}: Ужесточение фильтров (WR: {recent_win_rate:.1%})")
            
        # Если недавние результаты хорошие (> 70%)
        elif recent_win_rate > 0.7:
            # Можем немного ослабить фильтры
            optimized_params['min_confidence_threshold'] = max(40, optimized_params['min_confidence_threshold'] - 5)
            optimized_params['min_rr_ratio'] = max(1.1, optimized_params['min_rr_ratio'] - 0.1)
            logger.info(f"📈 {symbol}: Ослабление фильтров (WR: {recent_win_rate:.1%})")
            
        # Если средний PNL отрицательный
        if avg_pnl < -1:
            # Ужесточаем RR требования
            optimized_params['min_rr_ratio'] = min(3.0, optimized_params['min_rr_ratio'] + 0.2)
            logger.info(f"💸 {symbol}: Ужесточение RR (Avg PNL: {avg_pnl:.2f}%)")
            
        # Если средний PNL положительный и высокий (> 3%)
        elif avg_pnl > 3:
            # Можем немного снизить требования
            optimized_params['min_rr_ratio'] = max(1.0, optimized_params['min_rr_ratio'] - 0.1)
            logger.info(f"💰 {symbol}: Оптимизация RR (Avg PNL: {avg_pnl:.2f}%)")
            
        return optimized_params

    def get_adaptive_signal_requirements(self, symbol):
        """Адаптивные требования к сигналам"""
        base_requirements = {
            'min_long_conditions': 3,  # Минимум 3 из 6 условий
            'min_short_conditions': 3,
            'min_confidence': 45,
            'min_rr_ratio': 1.2,
            'require_volume_confirmation': True,
            'require_trend_confirmation': True,
            'require_momentum_confirmation': True
        }
        
        if symbol not in self.signal_stats:
            return base_requirements
            
        stats = self.signal_stats[symbol]
        recent_results = stats.get('recent_results', [])
        
        if len(recent_results) < 5:
            return base_requirements
            
        # Анализируем последние 5 сделок
        recent_performance = sum(recent_results[-5:]) / 5
        
        adaptive_requirements = base_requirements.copy()
        
        # Если последние сделки были убыточными
        if recent_performance < 0.4:
            adaptive_requirements['min_long_conditions'] = 4  # Ужесточаем требования
            adaptive_requirements['min_short_conditions'] = 4
            adaptive_requirements['min_confidence'] = 55
            adaptive_requirements['min_rr_ratio'] = 1.5
            
        # Если последние сделки были прибыльными
        elif recent_performance > 0.8:
            adaptive_requirements['min_long_conditions'] = 2  # Можем немного ослабить
            adaptive_requirements['min_short_conditions'] = 2
            adaptive_requirements['min_confidence'] = 40
            adaptive_requirements['min_rr_ratio'] = 1.1
            
        return adaptive_requirements

    def predict_signal_quality(self, symbol, signal, data_dict):
        """Прогнозирование качества сигнала на основе исторических данных"""
        if not signal or symbol not in self.signal_stats:
            return 0.5  # Нейтральная оценка
            
        try:
            stats = self.signal_stats[symbol]
            
            # Факторы прогноза:
            # 1. Исторический винрейт по символу
            historical_win_rate = stats.get('win_rate', 0.5)
            
            # 2. Недавние результаты
            recent_results = stats.get('recent_results', [])
            recent_win_rate = sum(recent_results[-10:]) / 10 if len(recent_results) >= 10 else historical_win_rate
            
            # 3. Качество технических условий сигнала
            tech_quality = self.evaluate_technical_quality(signal, data_dict)
            
            # 4. Рыночные условия
            market_conditions = self.evaluate_market_conditions(data_dict)
            
            # 5. Время суток (некоторые активы лучше работают в определенное время)
            time_factor = self.evaluate_time_factor()
            
            # Взвешенный прогноз
            quality_prediction = (
                historical_win_rate * 0.3 +      # 30% веса
                recent_win_rate * 0.2 +          # 20% веса
                tech_quality * 0.25 +           # 25% веса
                market_conditions * 0.15 +       # 15% веса
                time_factor * 0.1                # 10% веса
            )
            
            return max(0, min(1, quality_prediction))  # Ограничиваем 0-1
            
        except Exception as e:
            logger.error(f"Ошибка прогнозирования качества сигнала для {symbol}: {e}")
            return 0.5

    def evaluate_technical_quality(self, signal, data_dict):
        """Оценка технического качества сигнала"""
        if not signal or not data_dict or '1h' not in data_dict:
            return 0.5
            
        try:
            df_1h = data_dict['1h']
            if df_1h is None or len(df_1h) < 20:
                return 0.5
                
            # Оценка силы сигналов:
            rsi = float(df_1h['rsi'].iloc[-1])
            stoch_k = float(df_1h['stoch_k'].iloc[-1])
            stoch_d = float(df_1h['stoch_d'].iloc[-1])
            macd = float(df_1h['macd'].iloc[-1])
            macd_signal = float(df_1h['macd_signal'].iloc[-1])
            bb_position = float(df_1h['bb_position'].iloc[-1])
            
            quality_score = 0
            
            if signal['signal_type'] == 'LONG':
                # Оценка качества LONG сигнала
                # RSI в зоне перепроданности (сильнее = лучше)
                if 20 <= rsi <= 35:
                    quality_score += 0.3
                elif 15 <= rsi <= 20:
                    quality_score += 0.2
                elif 35 <= rsi <= 40:
                    quality_score += 0.1
                    
                # Стохастик в зоне перепроданности
                if stoch_k <= 25 and stoch_d <= 25:
                    quality_score += 0.2
                    
                # MACD бычий
                if macd > macd_signal:
                    quality_score += 0.2
                    
                # Цена в нижней части Боллинджера
                if bb_position <= 0.2:
                    quality_score += 0.3
                    
            else:  # SHORT
                # Оценка качества SHORT сигнала
                # RSI в зоне перекупленности
                if 65 <= rsi <= 80:
                    quality_score += 0.3
                elif 80 <= rsi <= 85:
                    quality_score += 0.2
                elif 60 <= rsi <= 65:
                    quality_score += 0.1
                    
                # Стохастик в зоне перекупленности
                if stoch_k >= 75 and stoch_d >= 75:
                    quality_score += 0.2
                    
                # MACD медвежий
                if macd < macd_signal:
                    quality_score += 0.2
                    
                # Цена в верхней части Боллинджера
                if bb_position >= 0.8:
                    quality_score += 0.3
                    
            return min(1.0, quality_score)
            
        except Exception as e:
            logger.error(f"Ошибка оценки технического качества: {e}")
            return 0.5

    def evaluate_market_conditions(self, data_dict):
        """Оценка рыночных условий"""
        if not data_dict or '1h' not in data_dict:
            return 0.5
            
        try:
            df_1h = data_dict['1h']
            if df_1h is None or len(df_1h) < 20:
                return 0.5
                
            volatility = float(df_1h['volatility'].iloc[-1]) if 'volatility' in df_1h.columns else 0.02
            volume_ratio = float(df_1h['volume_ratio'].iloc[-1]) if 'volume_ratio' in df_1h.columns else 1
            trend_strength = abs(float(df_1h['price_trend_20'].iloc[-1])) if 'price_trend_20' in df_1h.columns else 0.01
            
            # Оценка условий (0 = плохие, 1 = хорошие)
            condition_score = 0
            
            # Волатильность: оптимально 1-5%
            if 0.01 <= volatility <= 0.05:
                condition_score += 0.4
            elif 0.005 <= volatility <= 0.01 or 0.05 <= volatility <= 0.08:
                condition_score += 0.2
                
            # Объем: выше среднего лучше
            if volume_ratio >= 1.2:
                condition_score += 0.3
            elif volume_ratio >= 1.0:
                condition_score += 0.1
                
            # Трендовая сила: умеренная лучше
            if 0.01 <= trend_strength <= 0.05:
                condition_score += 0.3
            elif 0.005 <= trend_strength <= 0.01 or 0.05 <= trend_strength <= 0.1:
                condition_score += 0.1
                
            return min(1.0, condition_score)
            
        except Exception as e:
            logger.error(f"Ошибка оценки рыночных условий: {e}")
            return 0.5

    def evaluate_time_factor(self):
        """Оценка фактора времени суток"""
        try:
            current_hour = datetime.now().hour
            
            # Некоторые активы лучше работают в определенное время:
            # BTC: 24/7 - нейтрально
            # Азиатская сессия: 0-8 часов (лучше для азиатских активов)
            # Европейская сессия: 8-16 часов 
            # Американская сессия: 16-24 часа
            
            if 0 <= current_hour <= 8:  # Азиатская сессия
                return 0.6  # Средне
            elif 8 <= current_hour <= 16:  # Европейская сессия
                return 0.7  # Хорошо
            else:  # Американская сессия
                return 0.8  # Отлично
                
        except Exception as e:
            logger.error(f"Ошибка оценки временного фактора: {e}")
            return 0.5
            
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
            
            # Волатильность
            df['volatility'] = df['close'].pct_change().rolling(window=14).std() * np.sqrt(365)
            
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
        """БОЛЕЕ КОНСЕРВАТИВНЫЕ динамические уровни"""
        if not data_dict or '1h' not in data_dict:
            return self.calculate_basic_levels(symbol, data_dict, signal_type)
            
        df_1h = data_dict['1h']
        if df_1h is None or len(df_1h) < 20:
            return self.calculate_basic_levels(symbol, data_dict, signal_type)
            
        try:
            current_price = float(df_1h['close'].iloc[-1])
            atr = float(df_1h['atr'].iloc[-1]) if 'atr' in df_1h.columns and not pd.isna(df_1h['atr'].iloc[-1]) else current_price * 0.02
            
            # МЕНЬШЕ расстояния между уровнями
            base_sl_distance = atr * 1.1
            base_tp1_distance = atr * 1.6
            base_tp2_distance = atr * 2.4
            base_tp3_distance = atr * 3.2
            
            # МЕНЬШЕ множители
            potential_multiplier = 1.0
            
            if signal_type == 'LONG':
                sl = current_price - (base_sl_distance * 0.95)  # Меньше SL
                tp1 = current_price + (base_tp1_distance * potential_multiplier)
                tp2 = current_price + (base_tp2_distance * potential_multiplier * 1.02)
                tp3 = current_price + (base_tp3_distance * potential_multiplier * 1.05)
                
            else:  # SHORT
                sl = current_price + (base_sl_distance * 0.95)  # Меньше SL
                tp1 = current_price - (base_tp1_distance * potential_multiplier)
                tp2 = current_price - (base_tp2_distance * potential_multiplier * 1.02)
                tp3 = current_price - (base_tp3_distance * potential_multiplier * 1.05)
            
            # Проверка валидности
            rr_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
            if rr_ratio < 0.8 or rr_ratio > 8:  # Разумный диапазон
                raise Exception("Неразумный RR")
                
            if signal_type == 'LONG':
                if sl >= current_price or tp3 <= current_price:
                    raise Exception("Неразумные уровни LONG")
            else:  # SHORT
                if sl <= current_price or tp3 >= current_price:
                    raise Exception("Неразумные уровни SHORT")
                
            return round(float(sl), 8), round(float(tp1), 8), round(float(tp2), 8), round(float(tp3), 8)
            
        except Exception as e:
            logger.debug(f"⚠️  {symbol}: Использую базовые уровни из-за: {e}")
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
                # Проверка дополнительных фильтров
                if not self.check_multitimeframe_confirmation(data_dict, signal_type):
                    logger.debug(f"❌ {symbol}: Нет подтверждения на других таймфреймах")
                    return None
                    
                if not self.check_volatility_filter(df_1h):
                    logger.debug(f"❌ {symbol}: Не подходящая волатильность")
                    return None
                    
                if not self.check_volume_profile(data_dict):
                    logger.debug(f"❌ {symbol}: Недостаточный объем")
                    return None
                    
                if not self.check_correlation_filter(symbol, self.active_trades):
                    logger.debug(f"❌ {symbol}: Корреляция с активной позицией")
                    return None
                
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
            data_dict = self.robust_data_fetch(symbol, self.timeframes)
            if not data_dict:
                return None
                
            for tf in self.timeframes:
                if tf in data_dict and data_dict[tf] is not None:
                    data_dict[tf] = self.calculate_advanced_indicators(data_dict[tf], tf)
            
            multitimeframe_analysis = self.calculate_multitimeframe_analysis(data_dict)
            
            if symbol not in self.active_trades:
                signal = self.generate_signal(symbol, data_dict, multitimeframe_analysis)
                return signal
            else:
                logger.debug(f"⏭️  Пропущен {symbol} - уже есть активная сделка")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка обработки {symbol}: {e}")
            return None
            
    def backtest_strategy(self, symbol, days=30):
        """Простой бэктест стратегии с начальным балансом 100"""
        try:
            # Получение исторических данных
            ohlcv = self.fetch_ohlcv_with_cache(symbol, '1h', limit=days*24)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Расчет индикаторов
            df = self.calculate_advanced_indicators(df, '1h')
            
            if df is None or len(df) < 50:
                return None
                
            # Симуляция торговли
            signals = []
            position = None
            balance = 100  # Начальный баланс 100
            trades = []
            
            for i in range(50, len(df)):
                current_data = df.iloc[:i+1]
                
                # Создание временного data_dict для сигнала
                temp_data_dict = {'1h': current_data}
                
                # Генерация сигнала (используя существующую функцию)
                signal = self.generate_backtest_signal(symbol, temp_data_dict)
                
                if signal and position is None:
                    # Открытие позиции
                    position = {
                        'type': signal['signal_type'],
                        'entry_price': signal['entry_price'],
                        'sl': signal['sl'],
                        'tp1': signal['tp1'],
                        'tp2': signal['tp2'],
                        'tp3': signal['tp3'],
                        'balance_at_entry': balance
                    }
                    signals.append({
                        'timestamp': df.index[i],
                        'action': 'OPEN',
                        'signal': signal
                    })
                    
                elif position is not None:
                    current_price = float(df['close'].iloc[i])
                    
                    # Проверка закрытия позиции
                    exit_reason = None
                    exit_price = None
                    
                    if position['type'] == 'LONG':
                        if current_price <= position['sl']:
                            exit_reason = 'SL'
                            exit_price = position['sl']
                        elif current_price >= position['tp3']:
                            exit_reason = 'TP3'
                            exit_price = position['tp3']
                    else:  # SHORT
                        if current_price >= position['sl']:
                            exit_reason = 'SL'
                            exit_price = position['sl']
                        elif current_price <= position['tp3']:
                            exit_reason = 'TP3'
                            exit_price = position['tp3']
                    
                    if exit_reason:
                        # Закрытие позиции
                        if position['type'] == 'LONG':
                            pnl_percent = (exit_price - position['entry_price']) / position['entry_price'] * 100
                        else:  # SHORT
                            pnl_percent = (position['entry_price'] - exit_price) / position['entry_price'] * 100
                        
                        # Расчет прибыли/убытка в долларах (1% от баланса на сделку)
                        position_size = position['balance_at_entry'] * 0.01  # 1% от баланса
                        pnl_dollars = position_size * (pnl_percent / 100)
                        
                        balance += pnl_dollars
                        
                        trades.append({
                            'entry_price': position['entry_price'],
                            'exit_price': exit_price,
                            'pnl_percent': round(pnl_percent, 2),
                            'pnl_dollars': round(pnl_dollars, 2),
                            'balance_after_trade': round(balance, 2),
                            'reason': exit_reason
                        })
                        
                        signals.append({
                            'timestamp': df.index[i],
                            'action': 'CLOSE',
                            'reason': exit_reason,
                            'pnl_percent': round(pnl_percent, 2),
                            'pnl_dollars': round(pnl_dollars, 2)
                        })
                        
                        position = None
            
            # Расчет статистики
            if trades:
                total_trades = len(trades)
                winning_trades = len([t for t in trades if t['pnl_percent'] > 0])
                win_rate = winning_trades / total_trades if total_trades > 0 else 0
                avg_pnl = sum([t['pnl_percent'] for t in trades]) / total_trades if total_trades > 0 else 0
                total_pnl = sum([t['pnl_dollars'] for t in trades])
                final_balance = trades[-1]['balance_after_trade'] if trades else 100
                total_return = ((final_balance - 100) / 100) * 100  # Процентный доход
                
                return {
                    'initial_balance': 100,
                    'final_balance': round(final_balance, 2),
                    'total_return_percent': round(total_return, 2),
                    'total_trades': total_trades,
                    'win_rate': round(win_rate * 100, 2),
                    'avg_pnl_percent': round(avg_pnl, 2),
                    'total_pnl_dollars': round(total_pnl, 2),
                    'trades': trades,
                    'signals': signals
                }
            else:
                return {
                    'initial_balance': 100,
                    'final_balance': 100,
                    'total_return_percent': 0,
                    'total_trades': 0,
                    'win_rate': 0,
                    'avg_pnl_percent': 0,
                    'total_pnl_dollars': 0,
                    'trades': [],
                    'signals': []
                }
                
        except Exception as e:
            logger.error(f"Ошибка бэктеста для {symbol}: {e}")
            return None

    def generate_backtest_signal(self, symbol, data_dict):
        """Генерация сигнала для бэктеста"""
        # Используем существующую логику, но без сохранения состояния
        multitimeframe_analysis = self.calculate_multitimeframe_analysis(data_dict)
        return self.generate_signal(symbol, data_dict, multitimeframe_analysis)
        
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