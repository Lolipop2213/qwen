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
        
        # Кэш данных
        self.data_cache = {}
        self.cache_expiry = 300  # 5 минут
        
        # Имя файла для сохранения состояния
        self.state_file = 'bot_state.json'
        
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
            return 1.5
            
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
                base_rr = 1.3
            elif volatility < 0.01:  # Низкая волатильность
                base_rr = 1.8
                
            # Корректировка по тренду
            if trend_strength > 0.05:  # Сильный тренд
                base_rr *= 1.2
            else:
                base_rr *= 0.9
                
            return max(1.2, min(3.0, base_rr))
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
        
    def detect_candlestick_patterns(self, df):
        """Обнаружение паттернов свечей"""
        if df is None or len(df) < 10:
            return {}
            
        patterns = {}
        
        try:
            # Поглощение бычьее
            if (df['close'].iloc[-2] < df['open'].iloc[-2] and  # Предыдущая красная
                df['close'].iloc[-1] > df['open'].iloc[-1] and   # Текущая зеленая
                df['close'].iloc[-1] > df['open'].iloc[-2] and   # Закрытие выше открытия предыдущей
                df['open'].iloc[-1] < df['close'].iloc[-2]):     # Открытие ниже закрытия предыдущей
                patterns['bullish_engulfing'] = True
            
            # Поглощение медвежье
            if (df['close'].iloc[-2] > df['open'].iloc[-2] and  # Предыдущая зеленая
                df['close'].iloc[-1] < df['open'].iloc[-1] and   # Текущая красная
                df['close'].iloc[-1] < df['open'].iloc[-2] and   # Закрытие ниже открытия предыдущей
                df['open'].iloc[-1] > df['close'].iloc[-2]):     # Открытие выше закрытия предыдущей
                patterns['bearish_engulfing'] = True
            
            # Молот
            body = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
            upper_wick = df['high'].iloc[-1] - max(df['close'].iloc[-1], df['open'].iloc[-1])
            lower_wick = min(df['close'].iloc[-1], df['open'].iloc[-1]) - df['low'].iloc[-1]
            
            if (lower_wick > body * 2 and upper_wick < body * 0.5):
                patterns['hammer'] = True
            elif (upper_wick > body * 2 and lower_wick < body * 0.5):
                patterns['shooting_star'] = True
                
        except Exception as e:
            logger.error(f"Ошибка обнаружения паттернов: {e}")
            
        return patterns
        
    def calculate_market_sentiment(self, data_dict):
        """Расчет рыночного настроения"""
        sentiment = {
            'fear_greed_index': 50,
            'volatility_regime': 'normal',
            'volume_regime': 'normal'
        }
        
        if '1h' not in data_dict or data_dict['1h'] is None:
            return sentiment
            
        df = data_dict['1h']
        if len(df) < 20:
            return sentiment
            
        try:
            # Расчет Fear & Greed на основе:
            # 1. RSI (30% веса)
            rsi = float(df['rsi'].iloc[-1])
            rsi_score = max(0, min(100, (rsi - 30) * 2.5))
            
            # 2. Волатильность (30% веса)
            volatility = float(df['volatility'].iloc[-1]) if 'volatility' in df.columns else 0.02
            vol_score = max(0, min(100, volatility * 2000))
            
            # 3. Объем (20% веса)
            volume_ratio = float(df['volume_ratio'].iloc[-1])
            vol_ratio_score = max(0, min(100, volume_ratio * 50))
            
            # 4. Тренд (20% веса)
            trend_20 = float(df['price_trend_20'].iloc[-1])
            trend_score = max(0, min(100, (trend_20 + 0.1) * 500))
            
            sentiment['fear_greed_index'] = (
                rsi_score * 0.3 + 
                vol_score * 0.3 + 
                vol_ratio_score * 0.2 + 
                trend_score * 0.2
            )
            
            # Режим волатильности
            if volatility > 0.04:
                sentiment['volatility_regime'] = 'high'
            elif volatility < 0.01:
                sentiment['volatility_regime'] = 'low'
            else:
                sentiment['volatility_regime'] = 'normal'
                
            # Режим объема
            if volume_ratio > 1.5:
                sentiment['volume_regime'] = 'high'
            elif volume_ratio < 0.8:
                sentiment['volume_regime'] = 'low'
            else:
                sentiment['volume_regime'] = 'normal'
                
        except Exception as e:
            logger.error(f"Ошибка расчета настроения рынка: {e}")
            
        return sentiment
        
    def get_market_regime(self):
        """Определение рыночного режима"""
        try:
            # Получаем данные по BTC для определения общего тренда
            btc_data = self.fetch_ohlcv_with_cache('BTC/USDT', '1h', limit=50)
            if btc_data and len(btc_data) >= 20:
                df = pd.DataFrame(btc_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Рассчитываем волатильность BTC
                returns = df['close'].pct_change().dropna()
                volatility = returns.std() * np.sqrt(365)  # Годовая волатильность
                
                # Рассчитываем тренд BTC
                trend_20 = (df['close'].iloc[-1] - df['close'].iloc[-20]) / df['close'].iloc[-20]
                
                if volatility > 0.8:  # Высокая волатильность
                    return 'high_volatility'
                elif abs(trend_20) > 0.1:  # Сильный тренд (>10% за 20 часов)
                    return 'strong_trend'
                else:
                    return 'normal'
            else:
                return 'normal'
        except:
            return 'normal'

    def adjust_parameters_for_market_regime(self):
        """Адаптация параметров под рыночный режим"""
        regime = self.get_market_regime()
        
        if regime == 'high_volatility':
            # В высокой волатильности - больше сигналов, меньше RR
            self.risk_params['min_confidence_threshold'] = 40
            self.risk_params['min_rr_ratio'] = 1.1
        elif regime == 'strong_trend':
            # В сильном тренде - больше LONG/SHORT сигналов
            self.risk_params['min_confidence_threshold'] = 42
            self.risk_params['min_rr_ratio'] = 1.15
        else:
            # Нормальный режим
            self.risk_params['min_confidence_threshold'] = 45
            self.risk_params['min_rr_ratio'] = 1.2
            
    def should_apply_strict_filters(self):
        """Определяет, нужно ли применять строгие фильтры"""
        # Если уже много активных сделок - применяем строгие фильтры
        if len(self.active_trades) >= 8:  # Много позиций
            return True
        
        # Если недавно было много убыточных сделок - строже
        if self.get_recent_performance() < 0.4:  # Плохая недавняя статистика
            return True
        
        # В обычных условиях - мягче
        return False
        
    def get_recent_performance(self):
        """Получение недавней статистики"""
        if not self.signal_stats:
            return 0.5
            
        recent_results = []
        for stats in self.signal_stats.values():
            recent_results.extend(stats.get('recent_results', []))
        
        if len(recent_results) >= 10:
            return sum(recent_results[-10:]) / 10
        return 0.5
        
    def update_signal_statistics(self, symbol, signal_result, pnl=None):
        """Обновление статистики по сигналам"""
        if symbol not in self.signal_stats:
            self.signal_stats[symbol] = {
                'total_signals': 0,
                'winning_signals': 0,
                'total_pnl': 0,
                'win_rate': 0.0,
                'avg_pnl': 0.0,
                'recent_results': []  # Последние 20 результатов
            }
        
        stats = self.signal_stats[symbol]
        stats['total_signals'] += 1
        
        if signal_result == 'win':
            stats['winning_signals'] += 1
            if pnl:
                stats['total_pnl'] += pnl
            stats['recent_results'].append(1)
        else:
            stats['recent_results'].append(0)
        
        # Сохраняем только последние 20 результатов
        if len(stats['recent_results']) > 20:
            stats['recent_results'] = stats['recent_results'][-20:]
        
        stats['win_rate'] = stats['winning_signals'] / stats['total_signals']
        stats['avg_pnl'] = stats['total_pnl'] / stats['total_signals'] if stats['total_signals'] > 0 else 0
        
        # Рассчитываем недавний винрейт
        if len(stats['recent_results']) >= 10:
            recent_win_rate = sum(stats['recent_results'][-10:]) / 10
            stats['recent_win_rate'] = recent_win_rate

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
            else:
                sl = current_price + (base_sl_distance * 0.95)  # Меньше SL
                tp1 = current_price - (base_tp1_distance * potential_multiplier)
                tp2 = current_price - (base_tp2_distance * potential_multiplier * 1.02)
                tp3 = current_price - (base_tp3_distance * potential_multiplier * 1.05)
            
            # БОЛЕЕ ЛИБЕРАЛЬНАЯ проверка RR
            rr_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
            if rr_ratio < 0.6 or rr_ratio > 10:  # Шире диапазон
                raise Exception("Неразумный RR")
                
            return round(float(sl), 8), round(float(tp1), 8), round(float(tp2), 8), round(float(tp3), 8)
            
        except Exception as e:
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
            
            # БОЛЕЕ ЛИБЕРАЛЬНЫЕ условия для LONG
            long_conditions = [
                rsi < 45,  # Было 40
                stoch_k < 35 and stoch_d < 35,  # Было 30/30
                macd > macd_signal,
                bb_position < 0.35,  # Было 0.3
                volume_ratio > 1.0,  # Было 1.1
                momentum_1h > -0.02   # Было -0.015
            ]
            
            # БОЛЕЕ ЛИБЕРАЛЬНЫЕ условия для SHORT
            short_conditions = [
                rsi > 55,  # Было 60
                stoch_k > 65 and stoch_d > 65,
                macd < macd_signal,
                bb_position > 0.65,  # Было 0.7
                volume_ratio > 1.0,  # Было 1.1
                momentum_1h < 0.02   # Было 0.015
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
            
            # МЕНЬШЕ требований для открытия
            if long_score >= 2:  # Было 3 - теперь 2 из 6
                signal_type = 'LONG'
                confidence_score = (long_score / len(long_conditions)) * 100
            elif short_score >= 2 and self.risk_params['use_short_signals']:
                signal_type = 'SHORT'
                confidence_score = (short_score / len(short_conditions)) * 100
                
            # НИЖЕ порог уверенности
            if signal_type and confidence_score >= 35:  # Было 50
                # Применяем фильтры в зависимости от условий
                should_filter = self.should_apply_strict_filters()
                
                if should_filter:
                    # Строгие фильтры
                    if not self.check_multitimeframe_confirmation(data_dict, signal_type):
                        logger.debug(f"❌ {symbol}: Нет подтверждения (строгий режим)")
                        return None
                        
                    if not self.check_volatility_filter(df_1h):
                        logger.debug(f"❌ {symbol}: Не подходящая волатильность (строгий режим)")
                        return None
                        
                    if not self.check_volume_profile(data_dict):
                        logger.debug(f"❌ {symbol}: Недостаточный объем (строгий режим)")
                        return None
                        
                    if not self.check_correlation_filter(symbol, self.active_trades):
                        logger.debug(f"❌ {symbol}: Корреляция (строгий режим)")
                        return None
                else:
                    # Мягкие фильтры (только критические)
                    if not self.check_correlation_filter(symbol, self.active_trades):
                        logger.debug(f"❌ {symbol}: Корреляция (мягкий режим)")
                        return None
                
                # Рассчитываем динамические TP и SL
                sl, tp1, tp2, tp3 = self.calculate_dynamic_levels(symbol, data_dict, signal_type)
                
                if sl is None or tp1 is None or tp2 is None or tp3 is None:
                    logger.debug(f"❌ {symbol}: Ошибка расчета уровней")
                    return None
                
                # Проверка валидности уровней
                risk_reward_ratio = abs(tp3 - current_price) / (abs(current_price - sl) + 0.0001)
                
                # НОРМАЛЬНЫЙ RR ratio
                if signal_type == 'LONG' and sl < current_price and tp3 > current_price and risk_reward_ratio > 1.2:
                    valid = True
                elif signal_type == 'SHORT' and sl > current_price and tp3 < current_price and risk_reward_ratio > 1.2:
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
            ohlcv = self.exchange.fetch_ohlcv(symbol, '1h', limit=days*24)
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
        
        # Адаптируем параметры под рыночный режим
        self.adjust_parameters_for_market_regime()
        
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
        
    def send_alert(self, message, alert_type='info'):
        """Отправка алертов"""
        if alert_type == 'critical':
            logger.critical(f"🚨 КРИТИЧЕСКИЙ АЛЕРТ: {message}")
        elif alert_type == 'warning':
            logger.warning(f"⚠️  ПРЕДУПРЕЖДЕНИЕ: {message}")
        elif alert_type == 'success':
            logger.info(f"✅ УСПЕХ: {message}")
        else:
            logger.info(f"🔔 АЛЕРТ: {message}")

    def check_system_health(self):
        """Проверка состояния системы"""
        issues = []
        
        # Проверка интернет соединения
        try:
            import requests
            response = requests.get('https://api.binance.com/api/v3/ping', timeout=5)
            if response.status_code != 200:
                issues.append("Проблемы с подключением к Binance API")
        except:
            issues.append("Нет интернет соединения")
        
        # Проверка дискового пространства
        try:
            total, used, free = shutil.disk_usage("/")
            if free < 100 * 1024 * 1024:  # Менее 100MB
                issues.append("Мало свободного места на диске")
        except:
            issues.append("Ошибка проверки дискового пространства")
        
        # Проверка памяти
        try:
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                issues.append("Высокое использование памяти")
        except:
            issues.append("Ошибка проверки памяти")
        
        # Отправка алертов
        for issue in issues:
            self.send_alert(issue, 'warning')
        
        if not issues:
            self.send_alert("Система работает нормально", 'success')
        
        return len(issues) == 0
        
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
                
                # Проверка состояния системы каждые 10 циклов
                if cycle_count % 10 == 0:
                    self.check_system_health()
                
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