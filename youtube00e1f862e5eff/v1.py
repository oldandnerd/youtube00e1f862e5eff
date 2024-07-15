import re
from typing import AsyncGenerator
import aiohttp
import dateparser
import time
import asyncio
import requests
import random
import json
from bs4 import BeautifulSoup
from typing import AsyncGenerator
from datetime import datetime
from exorde_data import (
    Item,
    Content,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId
)
import logging
try:
    import nltk
    nltk.download('stopwords')
    stopwords = nltk.corpus.stopwords.words('english')
except Exception as e:
    logging.exception(f"[Youtube] nltk.corpus.stopwords.words('english') error: {e}")
    stopwords = []

"""
- Fetch https://www.youtube.com/results?search_query={KEYWORD} example: https://www.youtube.com/results?search_query=bitcoin
- Get all video URLs + their titles
- use youtube-comment library to extract all comments (with id, timestamp, and text)
- rebuild comment URLs from id, select those with recent timestamp
- add title to comment text (as first sentence).
- that's all folks
"""
MAX_TOTAL_COMMENTS_TO_CHECK = 150
PROBABILITY_ADDING_SUFFIX = 0.85
PROBABILITY_DEFAULT_KEYWORD = 0.4

DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 50
DEFAULT_MIN_POST_LENGTH = 10

DEFAULT_KEYWORDS = [   
    "paris2024",
    "paris2024",
    "olympic",
    "olympic", 
    "Acura",
    "Alfa Romeo",
    "Aston Martin",
    "Audi",
    "Bentley",
    "BMW",
    "Buick",
    "Cadillac",
    "Chevrolet",
    "Chrysler",
    "Dodge",
    "Ferrari",
    "Fiat",
    "Ford",
    "Genesis",
    "GMC",
    "Honda",
    "Hyundai",
    "Infiniti",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lotus",
    "Maserati",
    "Mazda",
    "McLaren",
    "Mercedes-Benz",
    "MINI",
    "Mitsubishi",
    "Nissan",
    "Porsche",
    "Ram",
    "Renault",
    "Rolls-Royce",
    "Subaru",
    "Tesla",
    "Toyota",
    "Volkswagen",
    "Volvo",    
    "BlackRock",
    "Vanguard",
    "State Street",
    "advisors",
    "Fidelity",
    "Fidelity Investments",
    "Asset Management",
    "Asset",
    "digital asset",
    "NASDAQ Composite",
    "Dow Jones Industrial Average",
    "Gold",
    "Silver",
    "Brent Crude",
    "WTI Crude",
    "EUR",
    "US",
    "YEN"
    "UBS",
    "PIMCO",
    "schroders",
    "aberdeen",    
    "louis vuitton",
    "moet Chandon",
    "hennessy",
    "dior",
    "fendi",
    "givenchy",
    "celine",
    "tag heuer",
    "bvlgari",
    "dom perignon",
    "hublot",
    "Zenith",        
    "data",
    "develop",
    "virtual",
    "automation",
    "algorithm",
    "code",
    "machine learning",
    "blockchain",
    "cybersecurity",
    "artificial",
    "synth",
    "synthetic",
    "major",
    "IoT",
    "cloud",
    "software",
    "API",
    "encryption",
    "quantum",
    "neural",
    "open source",
    "robotics",
    "devop",
    "5G",
    "virtual reality",
    "augmented reality",
    "bioinformatics",
    "vote",
    "vote",
    "vote",
    "election",
    "election",
    "election",
    "voter",
    "voter",
    "official",
    "million",
    "club",
    "tech",
    "nvda",
    "machine",
    "generative",
    "reinforcement",
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "TRUMP",
    "BabyDoge",
    "ERC20",
    "BONE",
    "COQ",
    "WEN",
    "BITCOIN",
    "ELON",
    "SNEK",
    "MYRO",
    "PORK",
    "TOSHI",
    "SMOG",
    "LADYS",
    "AIDOGE",
    "TURBO",
    "TOKEN",
    "SAMO",
    "KISHU",
    "TSUKA",
    "LEASH",
    "QUACK",
    "VOLT",
    "PEPE2.0",
    "JESUS",
    "MONA",
    "DC",
    "WSM",
    "PIT",
    "QOM",
    "PONKE",
    "SMURFCAT",
    "AKITA",
    "VINU",
    "ANALOS",
    "BAD",
    "CUMMIES",
    "HONK",
    "HOGE",
    "$MONG",
    "SHI",
    "BAN",
    "RAIN",
    "TAMA",
    "PAW",
    "SPX",
    "HOSKY",
    "BOZO",
    "DOBO",
    "PIKA",
    "CCC",
    "REKT",
    "WOOF",
    "MINU",
    "WOW",
    "PUSSY",
    "KEKE",
    "DOGGY",
    "KINGSHIB",
    "CHEEMS",
    "SMI",
    "OGGY",
    "DINGO",
    "DONS",
    "GRLC",
    "AIBB",
    "CATMAN",
    "XRP",
    "CAT",
    "数字資産",  # Digital Asset (Japanese)
    "仮想",  # Virtual (Japanese)
    "仮想通貨",  # Virtual Currency (Japanese)
    "自動化",  # Automation (Japanese)
    "アルゴリズム",  # Algorithm (Japanese)
    "コード",  # Code (Japanese)
    "機械学習",  # Machine Learning (Japanese)
    "ブロックチェーン",  # Blockchain (Japanese)
    "サイバーセキュリティ",  # Cybersecurity (Japanese)
    "人工",  # Artificial (Japanese)
    "合成",  # Synthetic (Japanese)
    "主要",  # Major (Japanese)
    "IoT",
    "クラウド",  # Cloud (Japanese)
    "ソフトウェア",  # Software (Japanese)
    "API",
    "暗号化",  # Encryption (Japanese)
    "量子",  # Quantum (Japanese)
    "ニューラルネットワーク",  # Neural Network (Japanese)
    "オープンソース",  # Open Source (Japanese)
    "ロボティクス",  # Robotics (Japanese)
    "デブオプス",  # DevOps (Japanese)
    "5G",
    "仮想現実",  # Virtual Reality (Japanese)
    "拡張現実",  # Augmented Reality (Japanese)
    "バイオインフォマティクス",  # Bioinformatics (Japanese)
    "ビッグデータ",  # Big Data (Japanese)
    "大統領",  # President (Japanese)
    "行政",  # Administration (Japanese)
    "Binance",
    "Bitcoin ETF",
    "政治",  # Politics (Japanese)
    "政治的",  # Political (Japanese)
    "ダイアグラム",  # Diagram (Japanese)
    "news",
    "news",
    "news stock crypto",
    "stock market news",
    "stock market news",
    "stock market news",
    "politic news",
    "breaking news",
    "press",
    "silentsunday",
    "breaking change",
    "tuesday" "bitcoin",
    "ethereum",
    "biden",
    "biden",
    "biden",
    "trump",
    "trump",
    "trump",
    "eth",
    "btc",
    "usdt",
    "cryptocurrency",
    "solana",
    "theft hack",
    "conspiracy",
    "new product",
    "new startup",
    "new startup",
    "emerging project",
    "doge",
    "cardano",
    "monero",
    "dogecoin",
    "polkadot",
    "ripple",
    "xrp",
    "stablecoin",
    "defi",
    "cbdc",
    "nasdaq",
    "sp500",
    "BNB",
    "ETF",
    "SpotETF",
    "iphone",
    "it",
    "usbc",
    "eu",
    "hack",
    "staking",
    "proof of work",
    "hacker",
    "hackers",
    "virtualreality",
    "metaverse",
    "tech",
    "technology",
    "art",
    "game",
    "trading",
    "groundnews",
    "breakingnews",
    "Gensler",
    "FED",
    "SEC",
    "IMF",
    "Macron",
    "macron",
    "macron",
    "macron",
    "Biden",
    "Putin",
    "Putin",
    "Putin",
    "Putin",
    "vladimir putin",
    "vladimir putin",
    "vladimir putin",
    "renault",
    "renault trucks",
    "volvo trucks",
    "Zelensky",
    "Trump",
    "legal",
    "bitcoiners",
    "bitcoincash",
    "ethtrading",
    "cryptonews",
    "cryptomarket",
    "cryptoart",
    "CPTPP",
    "brexit",
    "trade",
    "economy",
    "USpolitics",
    "UKpolitics",
    "NHL",
    "computer",
    "computerscience",
    "stem",
    "gpt4",
    "billgates",
    "ai",
    "chatgpt",
    "openai",
    "wissen",
    "french",
    "meat",
    "support",
    "aid",
    "mutualaid",
    "mastodon",
    "bluesky",
    "animal",
    "animalrights",
    "BitcoinETF",
    "Crypto",
    "altcoin",
    "DeFi",
    "GameFi",
    "web3",
    "web3",
    "trade",
    "NFT",
    "NFTs",
    "cryptocurrencies",
    "Cryptos",
    "reddit",
    "elon musk",
    "politics",
    "business",
    "twitter",
    "digital",
    "airdrop",
    "gamestop",
    "finance",
    "liquidity",
    "token",
    "economy",
    "markets",
    "stocks",
    "crisis",
    "gpt",
    "gpt3",
    "russia",
    "war",
    "ukraine",
    "luxury",
    "LVMH",
    "Elon musk",
    "conflict",
    "bank",
    "Gensler",
    "emeutes",
    "FaceID",
    "Riot",
    "riots",
    "riot",
    "France",
    "UnitedStates",
    "USA",
    "China",
    "Germany",
    "Europe",
    "Canada",
    "Mexico",
    "Brazil",
    "price",
    "market",
    "NYSE",
    "NASDAQ",
    "CAC",
    "CAC40",
    "G20",
    "OilPrice",
    "FTSE",
    "NYSE",
    "WallStreet",
    "money",
    "forex",
    "trading",
    "currency",
    "USD",
    "WarrenBuffett",
    "BlackRock",
    "Berkshire",
    "IPO",
    "Apple",
    "Tesla",
    "Alphabet",
    "FBstock",
    "debt",
    "bonds",
    "XAUUSD",
    "SP500",
    "DowJones",
    "satoshi",
    "shorts",    
    "Openfabric",
    "Openfabric AI",
    "Openfabric",
    "OFN",
    "live",
    "algotrading",
    "tradingalgoritmico",
    "prorealtime",
    "ig",
    "igmarkets",
    "win",
    "trading",
    "trader",
    "algorithm",
    "cfdauto",
    "algos",
    "bottrading",
    "tradingrobot",
    "robottrading",
    "prorealtimetrading",
    "algorithmictrading",
    "algorand",
    "algorand",
    "algorand",
    "algorand",
    "$algo",
    "$algo",
    "$algo",
    "digital asset",
    "coinbase",
    "binance",
    "coinbase",
    "binance",
    "coinbase",
    "binance",
    "kraken",
    "usa ",
    "canada ",
    "denmark",
    "russia",
    "japan",
    "italy",
    "spain",
    "uk",
    "eu",
    "social",
    "iran",
    "war",
    "socialism",
    "Biden",
    "democracy",
    "justice",
    "canada",
    "leftist",
    "SpaceX",
    "GreenEnergy",
    "CarbonCredits",
    "DeFiTokens",
    "Liquidity Mining",
    "NFT Marketplaces",
    "asset",
    "balance sheet",
    "bond",
    "budget",
    "cash flow",
    "compound interest",
    "credit",
    "debit",
    "debt",
    "dividend",
    "equity",
    "exchange rate",
    "expense",
    "financial advisor",
    "financial planning",
    "fiscal policy",
    "fixed income",
    "foreign exchange",
    "GDP",
    "gross domestic product",
    "income",
    "inflation",
    "insurance",
    "investment",
    "liability",
    "loan",
    "margin",
    "market capitalization",
    "mortgage",
    "mutual fund",
    "net income",
    "net worth",
    "portfolio",
    "price-to-earnings ratio",
    "profit",
    "recession",
    "retirement planning",
    "return on investment",
    "risk",
    "securities",
    "stock",
    "stock market",
    "tax",
    "treasury bill",
    "yield",
    "Web3Platforms",
    "DecentralizedIdentity",
    "CyberThreats",
    "AIResearch",
    "DeepLearning",
    "Bloomberg",
    "new technology",
    "new startup",
    "new erc20 token",
    "new crypto",
    "RoboticAutomation",
    "IoTDevices",
    "SpaceXLaunches",
    "CRISPRTechnology",
    "PrecisionMedicine",
    "new world order",
    "ElectricCars",
    "QuantumComputers",
    "AstronomyDiscoveries",
    "BiotechInnovations",
    "TelehealthServices",
    "RemoteCollaboration",
    "OTTStreaming",
    "GamingIndustry",
    "E-sportsTournaments",
    "EcoTourism",
    "AgriTech",
    "PlantBasedDiet",
    "MentalWellnessApps",
    "EdTech",
    "DigitalArtMarket",
    "CryptoPunks",
    "BlockchainScaling",
    "DeFiLending",
    "Tokenomics",
    "CryptoRegulation",
    "StablecoinTrends",
    "Cross-borderPayments",
    "ESGInvesting",
    "SustainableFinance",
    "SocialJusticeReforms",
    "DiversityInTech",
    "InclusiveWorkplace",
    "RemoteLearning",
    "VirtualConcerts",
    "CulinaryExploration",
    "MindfulnessPractice",
    "SpaceTourism",
    "Zero-EmissionVehicles",
    "SolarPowerInnovation",
    "BiofuelDevelopment",
    "AIEthics",
    "election",
    "vote",
    "protocol",
    "network",
    "org",
    "organization",
    "charity",
    "money",
    "scam",
    "token",
    "tokens",
    "ecosystem",
    "Bitcoin",
    "Ethereum",
    "Ripple",
    "Cardano",
    "Polkadot",
    "Solana",
    "Dogecoin",
    "Monero",
    "Chainlink",
    "Litecoin",
    "Stellar",
    "VeChain",
    "EOS",
    "Tron",
    "Tezos",
    "Cosmos",
    "Avalanche",
    "Neo",
    "Filecoin",
    "IOTA",
    "Uniswap",
    "Aave",
    "Compound",
    "Yearn Finance",
    "Synthetix",
    "Maker",
    "SushiSwap",
    "Curve Finance",
    "Balancer",
    "Alpha Finance",
    "Polygon",
    "Harmony",
    "Zilliqa",
    "Hedera Hashgraph",
    "Chia",
    "Theta",
    "Helium",
    "Internet Computer",
    "Algorand",
    "Celo",
    "Flow",
    "Decentraland",
    "Enjin Coin",
    "The Graph",
    "Basic Attention Token",
    "0x",
    "Ren",
    "SwissBorg",
    "Fetch.ai",
    "NKN",
    "Gold",
    "Silver",
    "Oil",
    "BrentCrude",
    "NaturalGas",
    "Copper",
    "Platinum",
    "Palladium",
    "Corn",
    "commodities market",
    "commodities prediction",
    "precious metals prediction",
    "fiat collapse",
    "digital asset reserve",
    "BRICS",
    "BRICS+",
    "BRICS currency",
    "darknet markets",
    "darknet currency",
    "rightwing",
    "DAX",
    "NASDAQ",
    "RUSSELL",
    "RUSSELL2000",
    "GOLD",
    "XAUUSD",
    "DAX40",
    "IBEX",
    "IBEX35",
    "oil",
    "crude",
    "crudeoil",
    "us500",
    "russell",
    "russell2000",
    "worldcoin",
    "sam atlman",
    "elon musk",
    "Biden administration",
    "Congress",
    "Supreme Court",
    "elections",
    "immigration",
    "healthcare",
    "climate change",
    "foreign policy",
    "war in Ukraine",
    "Roe v. Wade",
    "gun control",
    "inflation",
    "economy",
    "crime",
    "education",
    "abortion",
    "voting rights",
    "LGBTQ rights",
    "racial justice",
    "immigration reform",
    "gun violence prevention",
    "climate change action",
    "foreign aid",
    "national security",
    "trade",
    "technology",
    "infrastructure",
    "space exploration",
    "breaking news",
    "tech startups",
    "Silicon Valley",
    "venture capital",
    "unicorns",
    "IPO",
    "AI",
    "machine learning",
    "artificial intelligence",
    "deep learning",
    "robotics",
    "self-driving cars",
    "fintech",
    "e-commerce",
    "healthcare startups",
    "education startups",
    "climate tech startups",
    "space tech startups",
    "Web3",
    "cryptocurrency",
    "blockchain",
    "NFT",
    "decentralized finance", 
    "Israel",
    "Israel",
    "Israel",
    "Palestine",
    "Palestine",
    "Palestine",
    "jerusalem",
    "missile",
    "hostage",
    "attack",
    "hamas",
    "hamas",
    "hamas",
    "hamas",
    "gaza",
    "gaza",
    "gaza",
    "gaza",
    "syria",
    "middle east",
    "middle east",
    "middle east",
    "conflict",
    "missile",
    "hostage",
    "attack",
    "oil",
    "الاتحاد الأوروبي",
    "الذكاء الاصطناعي",
    "أخبار",
    "شركة ناشئة",
    "رسم بياني",
    "صراع",
    "تكنولوجيا",
    "شهير",
    "علاج",
    "فيروس",
    "وحدة معالجة الرسومات",
    "تعلم الآلة",
    "إيلون",
    "لعبة",
    "بلد",
    "سياسة",
    "صناعة",
    "أعمال",
    "شركة",
    "فوز",
    "خسارة",
    "ضائع",
    "ربح",
    "توقع",
    "شائعة",
    "صحة",
    "ترفيه",
    "بث مباشر",
    "فخامة",
    "علامة تجارية",
    "منتج",
    "ЕС",
    "ИИ",
    "новости",
    "стартап",
    "график",
    "конфликт",
    "технологии",
    "популярный",
    "лечение",
    "вирус",
    "ГПУ",
    "Машинное обучение",
    "Илон",
    "Игра",
    "Страна",
    "политика",
    "индустрия",
    "бизнес",
    "компания",
    "победа",
    "поражение",
    "потеря",
    "прибыль",
    "прогноз",
    "слух",
    "здоровье",
    "развлечение",
    "потоковая передача",
    "роскошь",
    "бренд",
    "продукт",
    "UE",
    "IA",
    "noticias",
    "startup",
    "gráfico",
    "conflicto",
    "tecnología",
    "popular",
    "cura",
    "virus",
    "GPU",
    "Aprendizaje Automático",
    "Elon",
    "Juego",
    "País",
    "política",
    "industria",
    "negocios",
    "empresa",
    "ganar",
    "perder",
    "perdido",
    "ganancia",
    "predicción",
    "rumor",
    "salud",
    "entretenimiento",
    "streaming",
    "lujo",
    "marca",
    "producto",
    "ニュース",
    "スタートアップ",
    "チャート",
    "紛争",
    "テクノロジー",
    "人気",
    "治療",
    "ウイルス",
    "GPU",
    "機械学習",
    "イーロン",
    "ゲーム",
    "国",
    "政治",
    "産業",
    "ビジネス",
    "会社",
    "勝利",
    "敗北",
    "失われた",
    "利益",
    "予測",
    "噂",
    "健康",
    "エンターテインメント",
    "ストリーミング",
    "高級",
    "ブランド",
    "製品",
    "欧盟",
    "人工智能",
    "新闻",
    "初创企业",
    "图表",
    "冲突",
    "技术",
    "热门",
    "治疗",
    "病毒",
    "机器学习",
    "埃隆",
    "游戏",
    "国家",
    "政治",
    "行业",
    "业务",
    "公司",
    "赢",
    "输",
    "失去",
    "收益",
    "预测",
    "谣言",
    "健康",
    "娱乐",
    "流媒体",
    "豪华",
    "品牌",
    "产品",
    "UE",
    "IA",
    "actualité",
    "démarrage",
    "graphique",
    "conflit",
    "technologie",
    "populaire",
    "traitement",
    "virus",
    "apprentissage",
    "Elon",
    "jeu",
    "pays",
    "politique",
    "industrie",
    "entreprise",
    "gagner",
    "perdre",
    "perdu",
    "gain",
    "prédiction",
    "rumeur",
    "santé",
    "divertissement",
    "diffusion",
    "luxe",
    "marque",
    "produit",
    "incroyable",
    "EU",
    "KI",
    "Nachrichten",
    "Start-up",
    "Diagramm",
    "Konflikt",
    "Technologie",
    "beliebt",
    "Heilung",
    "Virus",
    "Maschinelles Lernen",
    "Elon",
    "Spiel",
    "Land",
    "Politik",
    "Branche",
    "Geschäft",
    "Firma",
    "Gewinn",
    "Verlust",
    "verloren",
    "Gewinn",
    "Vorhersage",
    "Gerücht",
    "Gesundheit",
    "Unterhaltung",
    "Streaming",
    "Luxus",
    "Marke",
    "Produkt",
    "incroyable",
    "choquant",
    "mon Dieu",
    "achetez",
    "unglaublich",
    "schockierend",
    "mein Gott",
    "kaufen",
    "increíble",
    "impactante",
    "Dios mío",
    "compra",
    "رائع",
    "صادم",
    "يا إلهي",
    "اشتر الآن",
    "невероятный",
    "шокирующий",
    "о боже мой",
    "купи сейчас",
    "信じられない",
    "衝撃的",
    "ああ、神様",
    "今買う",
    "令人难以置信",
    "令人震惊",
    "哦，我的天啊",
    "立刻购买",
    "increíble",
    "impactante"
]

global YT_COMMENT_DLOADER_
YT_COMMENT_DLOADER_ = None

###### --- YOUTUBE COMMENT DOWNLOADER --- ########
NB_AJAX_CONSECUTIVE_MAX_TRIALS = 15
REQUEST_TIMEOUT = 8
POST_REQUEST_TIMEOUT = 4
SORT_BY_POPULAR = 0
SORT_BY_RECENT = 1


YOUTUBE_VIDEO_URL = 'https://www.youtube.com/watch?v={youtube_id}'
YOUTUBE_CONSENT_URL = 'https://consent.youtube.com/save'

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

SORT_BY_POPULAR = 0
SORT_BY_RECENT = 1

YT_CFG_RE = r'ytcfg\.set\s*\(\s*({.+?})\s*\)\s*;'
YT_INITIAL_DATA_RE = r'(?:window\s*\[\s*["\']ytInitialData["\']\s*\]|ytInitialData)\s*=\s*({.+?})\s*;\s*(?:var\s+meta|</script|\n)'
YT_HIDDEN_INPUT_RE = r'<input\s+type="hidden"\s+name="([A-Za-z0-9_]+)"\s+value="([A-Za-z0-9_\-\.]*)"\s*(?:required|)\s*>'

class YoutubeCommentDownloader:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = USER_AGENT
        self.session.cookies.set('CONSENT', 'YES+cb', domain='.youtube.com')
            
    def ajax_request(self, endpoint, ytcfg, retries=5, sleep=15 ):
        url = 'https://www.youtube.com' + endpoint['commandMetadata']['webCommandMetadata']['apiUrl']

        data = {'context': ytcfg['INNERTUBE_CONTEXT'],
                'continuation': endpoint['continuationCommand']['token']}

        for _ in range(retries):
            response = self.session.post(url, params={'key': ytcfg['INNERTUBE_API_KEY']}, json=data, timeout=POST_REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json()
            if response.status_code in [403, 413]:
                return {}
            else:
                time.sleep(sleep)

    def get_comments(self, youtube_id, *args, **kwargs):
        return self.get_comments_from_url(YOUTUBE_VIDEO_URL.format(youtube_id=youtube_id), *args, **kwargs)

    def get_comments_from_url(self, youtube_url, sort_by=SORT_BY_RECENT, language=None, sleep=.1, limit=100, max_oldness_seconds=3600):
        response = self.session.get(youtube_url, timeout=REQUEST_TIMEOUT)

        if 'consent' in str(response.url):
            # We may get redirected to a separate page for cookie consent. If this happens we agree automatically.
            params = dict(re.findall(YT_HIDDEN_INPUT_RE, response.text))
            params.update({'continue': youtube_url, 'set_eom': False, 'set_ytc': True, 'set_apyt': True})
            response = self.session.post(YOUTUBE_CONSENT_URL, params=params, timeout=REQUEST_TIMEOUT)

        html = response.text
        ytcfg = json.loads(self.regex_search(html, YT_CFG_RE, default=''))
        if not ytcfg:
            return  # Unable to extract configuration
        if language:
            ytcfg['INNERTUBE_CONTEXT']['client']['hl'] = language

        data = json.loads(self.regex_search(html, YT_INITIAL_DATA_RE, default=''))

        item_section = next(self.search_dict(data, 'itemSectionRenderer'), None)
        renderer = next(self.search_dict(item_section, 'continuationItemRenderer'), None) if item_section else None
        if not renderer:
            # Comments disabled?
            return

        sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu:
            # No sort menu. Maybe this is a request for community posts?
            section_list = next(self.search_dict(data, 'sectionListRenderer'), {})
            continuations = list(self.search_dict(section_list, 'continuationEndpoint'))
            # Retry..
            data = self.ajax_request(continuations[0], ytcfg) if continuations else {}
            sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu or sort_by >= len(sort_menu):
            raise RuntimeError('Failed to set sorting')
        continuations = [sort_menu[sort_by]['serviceEndpoint']]

        comment_counter = 0
        old_comment_counter = 0
        break_condition = False
        while continuations:
            if break_condition:
                break

            continuation = continuations.pop()
            response = self.ajax_request(continuation, ytcfg)

            if not response:
                break

            error = next(self.search_dict(response, 'externalErrorMessage'), None)
            if error:
                raise RuntimeError('Error returned from server: ' + error)

            actions = list(self.search_dict(response, 'reloadContinuationItemsCommand')) + \
                      list(self.search_dict(response, 'appendContinuationItemsAction'))
            for action in actions:
                for item in action.get('continuationItems', []):
                    if action['targetId'] in ['comments-section',
                                              'engagement-panel-comments-section',
                                              'shorts-engagement-panel-comments-section']:
                        # Process continuations for comments and replies.
                        continuations[:0] = [ep for ep in self.search_dict(item, 'continuationEndpoint')]
                    if action['targetId'].startswith('comment-replies-item') and 'continuationItemRenderer' in item:
                        # Process the 'Show more replies' button
                        continuations.append(next(self.search_dict(item, 'buttonRenderer'))['command'])

            toolbar_payloads = self.search_dict(response, 'engagementToolbarStateEntityPayload')
            toolbar_states = {payloads['key']:payloads for payloads in toolbar_payloads}
            for comment in reversed(list(self.search_dict(response, 'commentEntityPayload'))):
                properties = comment['properties']
                author = comment['author']
                toolbar = comment['toolbar']
                toolbar_state = toolbar_states[properties['toolbarStateKey']]
                result = {'cid': properties['commentId'],
                          'text': properties['content']['content'],
                          'time': properties['publishedTime'],
                          'author': author['displayName'],
                          'channel': author['channelId'],
                          'votes': toolbar['likeCountLiked'],
                          'replies': toolbar['replyCount'],
                          'photo': author['avatarThumbnailUrl'],
                          'heart': toolbar_state.get('heartState', '') == 'TOOLBAR_HEART_STATE_HEARTED',
                          'reply': '.' in properties['commentId']}

                try:
                    result['time_parsed'] = dateparser.parse(result['time'].split('(')[0].strip()).timestamp()
                except AttributeError:
                    pass

                paid = (
                    comment.get('paidCommentChipRenderer', {})
                    .get('pdgCommentChipRenderer', {})
                    .get('chipText', {})
                    .get('simpleText')
                )
                if paid:
                    result['paid'] = paid

                comment_counter += 1
                # break if we have enough comments
                if comment_counter >= limit:
                    logging.info(f"[Youtube] Comment limit reached: {limit} newest comments found. Moving on...")
                    break_condition = True
                    break
                # break if the comment is too old
                if result['time_parsed'] < time.time() - max_oldness_seconds:
                    old_comment_counter += 1
                    if old_comment_counter > 10:
                        logging.info(f"[Youtube] The most recent comments are too old, moving on...")
                        break_condition = True
                    break

                yield result
            time.sleep(sleep)

    @staticmethod
    def regex_search(text, pattern, group=1, default=None):
        match = re.search(pattern, text)
        return match.group(group) if match else default

    @staticmethod
    def search_dict(partial, search_key):
        stack = [partial]
        while stack:
            current_item = stack.pop()
            if isinstance(current_item, dict):
                for key, value in current_item.items():
                    if key == search_key:
                        yield value
                    else:
                        stack.append(value)
            elif isinstance(current_item, list):
                stack.extend(current_item)


def is_within_timeframe_seconds(input_timestamp, timeframe_sec):
    input_timestamp = int(input_timestamp)
    current_timestamp = int(time.time())  # Get the current UNIX timestamp
    elapsed_time = current_timestamp - input_timestamp

    if elapsed_time <= timeframe_sec:
        return True
    else:
        return False
    
def extract_url_parts(urls):
    result = []
    for url in urls:
        # Split the URL at the '&' character and keep only the first part
        url_part = url.split('&')[0]
        result.append(url_part)
    return result

def convert_timestamp(timestamp):
    dt = datetime.utcfromtimestamp(int(timestamp))
    formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    return formatted_dt

def randomly_add_search_filter(input_URL, p):
    # dict mapping suffixes with what they do
    suffixes_dict = {
        "&sp=CAI%253D": "newest videos",
        "&sp=CAASAhAB": "relevant videos",
        "&sp=EgQIARAB": "last hour videos",
        "&sp=EgQIAhAB": "last day videos",
        "&sp=EgQIAxAB": "last week videos",
    }
    suffixes_list = list(suffixes_dict.keys())
    if random.random() < p:
        # Choose one of the suffixes based on probability distribution
        chosen_suffix = random.choices(suffixes_list, weights=[0.10, 0.15, 0.25, 0.25, 0.25])[0]
        logging.info(f"[Youtube] Adding search filter to URL:  {suffixes_dict[chosen_suffix]}")
        return input_URL + chosen_suffix
    else:
        return input_URL
    
async def scrape(keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check):
    global YT_COMMENT_DLOADER_
    URL = "https://www.youtube.com/results?search_query={}".format(keyword)
    URL = randomly_add_search_filter(URL, p=PROBABILITY_ADDING_SUFFIX)
    logging.info(f"[Youtube] Looking at video URL: {URL}")

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        try:
            async with session.get(URL, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                html = await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"An error occurred during the request: {e}")
            return

    soup = BeautifulSoup(html, 'html.parser')

    URLs_remaining_trials = 10
    await asyncio.sleep(2)
    # Find the script tag containing the JSON data
    script_tag = soup.find('script', string=lambda text: text and 'var ytInitialData' in text)

    urls = []
    titles = []
    if script_tag:
        await asyncio.sleep(0.1) 
        # Extract the JSON content
        json_str = str(script_tag)
        start_index = json_str.find('var ytInitialData = ') + len('var ytInitialData = ')
        end_index = json_str.rfind('};') + 1
        json_data_str = json_str[start_index:end_index]

        try:
            # Parse the JSON data
            data = json.loads(json_data_str)
            # Extract titles and URLs
            if 'contents' in data:
                logging.info("[Youtube] Parsing search page: raw contents found...")
                primary_contents = data['contents']['twoColumnSearchResultsRenderer']['primaryContents']
                for item in primary_contents['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents']:
                    if 'videoRenderer' in item:
                        video = item['videoRenderer']
                        title = video['title']['runs'][0]['text']
                        url_suffix = video['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url']
                        full_url = f"https://www.youtube.com{url_suffix}"
                        urls.append(full_url)
                        titles.append(title)
                        logging.info(f"[Youtube] Video URL found = {full_url} and title: {title}")

        except json.JSONDecodeError as e:
            logging.info(f"[Youtube] Invalid JSON data in var ytInitialData: {e}")
    else:
        logging.info("[Youtube] No ytInitialData found.")


    last_n_video_comment_count = []
    n_rolling_size = 8
    n_rolling_size_min = 3

    yielded_items = 0
    nb_comments_checked = 0

    urls = extract_url_parts(urls)
    try:
        # shuffle the urls and titles together mapped 
        urlstitles = list(zip(urls, titles))
        random.shuffle(urlstitles)
        urls, titles = zip(*urlstitles)
    except Exception as e:
        # if urls or titles are empty, zip will throw an error
        if len(urls) == 0 or len(titles) == 0:
            logging.info(f"[Youtube] urls or titles is empty, skipping...")
        else:
            logging.exception(f"[Youtube] zip(*urlstitles) error: {e}")
        return
    
    for url, title in zip(urls, titles):
        await asyncio.sleep(1) 
        # skip URL randomly with 10% chance
        if random.random() < 0.1:
            logging.info(f"[Youtube] Randomly skipping URL: {url}")
            continue
        youtube_video_url = url
        # Run the generator function and handle the timeout
        comments_list = []       
        ###################################################################
        # Exponential backoff to prevent rate limiting
        # use a polynomial formula based on the number of 0 in last_n_video_comment_count
        # f(nb_zeros) = 3 + nb_zeros^2
        nb_zeros = 0
        # iterate from the end of the array, count consecutive 0 and break when we find a non 0
        if len(last_n_video_comment_count) >= n_rolling_size_min:
            for i in range(len(last_n_video_comment_count)-1, -1, -1):
                if last_n_video_comment_count[i] == 0:
                    nb_zeros += 1
                else:
                    break
                # compute the sleep time
            random_inter_sleep = round(0.1 + nb_zeros*0.2,1) ## 1.5 is the exponent
            logging.info(f"[Youtube] [soft rate limit] Waiting  {random_inter_sleep} seconds...")
            await asyncio.sleep(random_inter_sleep)
        ###################################################################

        try:
            logging.info(f"[Youtube] Getting ...{url}")
            comments_list = YT_COMMENT_DLOADER_.get_comments_from_url(url, sort_by=SORT_BY_RECENT, max_oldness_seconds=max_oldness_seconds)

            ###### ROLLING WINDOWS OF COMMENTS COUNT ######
            ### ADD LATEST COMMENTS COUNT TO THE ROLLING WINDOW
            # turn generator into list
            comments_list = list(comments_list)
            nb_comments = len(comments_list)
            nb_comments_checked += nb_comments  
            logging.info(f"[Youtube] Found {nb_comments} recent comments on video: {title}")
            last_n_video_comment_count.append(len(comments_list))
            ### REMOVE THE OLDEST COMMENTS COUNT FROM THE ROLLING WINDOW
            if len(last_n_video_comment_count) > n_rolling_size:
                last_n_video_comment_count.pop(0)
            ### CHECK IF THE ROLLING WINDOW IS FULL
            if len(last_n_video_comment_count) == n_rolling_size:
                ### CHECK IF THE ROLLING WINDOW IS FULL OF 0
                if sum(last_n_video_comment_count) == 0:
                    ### IF YES, STOP THE PROCESS
                    logging.info("[Youtube] [RATE LIMITE PROTECTION] The rolling window of comments count is full of 0s. Stopping the scraping iteration...")
                    break
        except Exception as e:      
            logging.exception(f"[Youtube] YT_COMMENT_DLOADER_ - ERROR: {e}")
            # Wait for a random amount of time between 10 and 30 seconds
            random_inter_sleep = round(3 + random.random()*7,1)
            logging.info(f"[Youtube] Waiting  {random_inter_sleep} seconds after the error...")
            await asyncio.sleep(random_inter_sleep)

        for comment in comments_list:
            try:
                comment_timestamp = int(round(comment['time_parsed'],1))
            except Exception as e:
                logging.exception(f"[Youtube] parsing comment datetime error: {e}\n \
                THIS CAN BE DUE TO FOREIGN/SPECIAL DATE FORMAT, not handled at this date.\n Please report this to the Exorde discord, with your region/VPS location.")

            comment_url = youtube_video_url + "&lc=" +  comment['cid']
            comment_id = comment['cid']
            # skip if text is too small
            if len(comment['text']) < 5:
                continue
            # make a titled_context from the title of the video, without special characters and punctuation
            # randomly remove some words from the title & stop words        
            try:
                title_base = " ".join([word for word in title.split(" ") if word not in stopwords])
                titled_context = title_base
            except Exception as e:
                logging.exception(f"[Youtube] stopwords error: {e}")
                titled_context = title
            if random.random() < 0.3:
                # remove up to 40% of the title
                titled_context = " ".join([word for word in title.split(" ") if random.random() > 0.3])
            elif random.random() < 0.4:
                # remove up to 20% of the title
                titled_context = " ".join([word for word in title.split(" ") if random.random() > 0.2])
            # remove non alpha-numeric characters that are single words
            titled_context = " ".join([word for word in titled_context.split(" ") if word.isalnum() and len(word) > 1])
            # add a dot at the end
            comment_content = titled_context + ". " + comment['text']
            comment_datetime = convert_timestamp(comment_timestamp)
            if is_within_timeframe_seconds(comment_timestamp, max_oldness_seconds):
                comment_obj = {'url':comment_url, 'content':comment_content, 'title':title, 'created_at':comment_datetime, 'external_id':comment_id}
                logging.info(f"[Youtube] found new comment: {comment_obj}")
                yield Item(
                    content=Content(str(comment_content)),
                    created_at=CreatedAt(str(comment_obj['created_at'])),
                    title=Title(str(comment_obj['title'])),
                    domain=Domain("youtube.com"),
                    url=Url(comment_url),
                    external_id=ExternalId(str(comment_obj['external_id']))
                )
                yielded_items += 1
                if yielded_items >= maximum_items_to_collect:
                    break
        if nb_comments_checked >= max_total_comments_to_check:
            break
        
        URLs_remaining_trials -= 1
        if URLs_remaining_trials <= 0:
            break
            
def randomly_replace_or_choose_keyword(input_string, p):
    if random.random() < p:
        return input_string
    else:
        return random.choice(DEFAULT_KEYWORDS)

def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

        try:
            probability_to_select_default_kws = parameters.get("probability_to_select_default_kws", PROBABILITY_DEFAULT_KEYWORD)
        except KeyError:
            probability_to_select_default_kws = PROBABILITY_DEFAULT_KEYWORD

        try:
            max_total_comments_to_check = parameters.get("max_total_comments_to_check", MAX_TOTAL_COMMENTS_TO_CHECK)
        except KeyError:
            max_total_comments_to_check = MAX_TOTAL_COMMENTS_TO_CHECK


    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        probability_to_select_default_kws = PROBABILITY_DEFAULT_KEYWORD
        max_total_comments_to_check = MAX_TOTAL_COMMENTS_TO_CHECK

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, probability_to_select_default_kws, max_total_comments_to_check


def convert_spaces_to_plus(input_string):
    return input_string.replace(" ", "+")

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    global YT_COMMENT_DLOADER_
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length, probability_to_select_default_kws, max_total_comments_to_check  = read_parameters(parameters)
    selected_keyword = ""
    YT_COMMENT_DLOADER_ = YoutubeCommentDownloader()
    
    content_map = {}
    await asyncio.sleep(1)
    try:
        if "keyword" in parameters:
            selected_keyword = parameters["keyword"]
        # replace it, with some probability, by a main default keyword
        selected_keyword = randomly_replace_or_choose_keyword(selected_keyword, p=probability_to_select_default_kws)
        selected_keyword = convert_spaces_to_plus(selected_keyword)
    except Exception as e:
        logging.exception(f"[Youtube parameters] parameters: {parameters}. Error when reading keyword: {e}")        
        selected_keyword = randomly_replace_or_choose_keyword("", p=1)

    logging.info(f"[Youtube] - Scraping latest comments posted less than {max_oldness_seconds} seconds ago, on youtube videos related to keyword: {selected_keyword}.")
    try:
        async for item in scrape(selected_keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check):
            # check if the content is not already in the map
            if item['content'] in content_map:
                continue
            else:
                content_map[item['content']] = True
            # check if the content is not too short
            if len(item['content']) < min_post_length:
                continue
            yielded_items += 1
            yield item
            if yielded_items >= maximum_items_to_collect:
                break
    except asyncio.exceptions.TimeoutError:
        logging.info(f"[Youtube] Internal requests are taking longer than {REQUEST_TIMEOUT} - we must give up & move on. Check your network.")
