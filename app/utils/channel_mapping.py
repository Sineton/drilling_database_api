"""
Channel mapping utilities for LAS files
"""
from typing import Dict, Optional, List


class ChannelMapper:
    """Utility class for LAS channel mapping"""
    
    # Standard channel mappings
    STANDARD_MAPPINGS = {
        # Depth
        "DEPT": "dmea",
        "DEPTH": "dmea",
        "MD": "dmea",
        "TVD": "tvd",
        "DBTM": "dbtm",
        "Zab": "dbtm",
        "Gl.dol": "dmea",
        
        # Weight and load
        "WOB": "wob",
        "W": "wob",
        "HKLD": "hkld",
        "HOOKLOAD": "hkld",
        "W kr": "hkld",
        "Ves instr.": "hkld",
        
        # Rotation
        "RPM": "rpm",
        "N rot": "rpm",
        "RPMB": "rpm",
        
        # Torque
        "TRQ": "trq",
        "TORQUE": "trq",
        "M": "trq",
        "M kl": "trq",
        
        # Rate of penetration
        "ROP": "rop",
        "ROPA": "rop",
        
        # Pressure
        "SPP": "spp",
        "SPPA": "spp",
        "P vkh": "spp",
        
        # Flow
        "MFIP": "mfip",
        "FLOWIN": "mfip",
        "Q vkh": "mfip",
        "MFOP": "mfop",
        "FLOWOUT": "mfop",
        "Q vyikh": "mfop",
        
        # Mud weight
        "MWIN": "mwin",
        "MWDIN": "mwin",
        "G vkh": "mwin",
        "MWOP": "mwop",
        "MWDOUT": "mwop",
        "G vyikh": "mwop",
        
        # Temperature
        "MTIA": "mtia",
        "TEMPIN": "mtia",
        "MTOA": "mtoa",
        "TEMPOUT": "mtoa",
        
        # Volume
        "TVOL": "tvol",
        "V sum": "tvol",
        
        # Pumps
        "SPM1": "spm1",
        "SPM2": "spm2",
        
        # Gas
        "TGAS": "tgas",
        "GAS": "tgas",
        "G sum": "tgas",
        "C1C5": "c1c5",
        
        # Position
        "BPOS": "bpos",
        "BITPOS": "bpos",
        "Hkr": "bpos",
    }
    
    # Channel descriptions (Russian)
    CHANNEL_DESCRIPTIONS = {
        "dmea": "Глубина по стволу (MD)",
        "tvd": "Вертикальная глубина (TVD)",
        "dbtm": "Глубина забоя",
        "wob": "Нагрузка на долото",
        "hkld": "Нагрузка на крюке",
        "rpm": "Обороты ротора",
        "trq": "Крутящий момент",
        "rop": "Механическая скорость",
        "spp": "Давление в стояке",
        "mfip": "Расход на входе",
        "mfop": "Расход на выходе",
        "mwin": "Плотность раствора (вход)",
        "mwop": "Плотность раствора (выход)",
        "mtia": "Температура раствора (вход)",
        "mtoa": "Температура раствора (выход)",
        "tvol": "Общий объём",
        "spm1": "Ходы насоса 1",
        "spm2": "Ходы насоса 2",
        "tgas": "Общий газ",
        "c1c5": "Газ C1-C5",
        "bpos": "Положение долота",
    }
    
    @classmethod
    def get_mapping(cls, las_mnemonic: str) -> Optional[str]:
        """Get database column name for LAS mnemonic"""
        return cls.STANDARD_MAPPINGS.get(las_mnemonic)
    
    @classmethod
    def suggest_mapping(cls, las_mnemonic: str) -> Optional[str]:
        """Suggest mapping for unknown mnemonic"""
        mnemonic_upper = las_mnemonic.upper()
        
        # Try exact match first
        if las_mnemonic in cls.STANDARD_MAPPINGS:
            return cls.STANDARD_MAPPINGS[las_mnemonic]
        
        # Try case-insensitive
        for key, value in cls.STANDARD_MAPPINGS.items():
            if key.upper() == mnemonic_upper:
                return value
        
        # Try partial match
        for key, value in cls.STANDARD_MAPPINGS.items():
            if key.upper() in mnemonic_upper or mnemonic_upper in key.upper():
                return value
        
        return None
    
    @classmethod
    def get_description(cls, db_column: str) -> str:
        """Get Russian description for database column"""
        return cls.CHANNEL_DESCRIPTIONS.get(db_column, db_column)
    
    @classmethod
    def build_mapping(cls, las_curves: List[str], custom_mapping: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build mapping dictionary for list of LAS curves"""
        mapping = {}
        
        for curve in las_curves:
            # Check custom mapping first
            if custom_mapping and curve in custom_mapping:
                mapping[curve] = custom_mapping[curve]
            else:
                # Try standard mapping
                db_col = cls.suggest_mapping(curve)
                if db_col:
                    mapping[curve] = db_col
        
        return mapping
