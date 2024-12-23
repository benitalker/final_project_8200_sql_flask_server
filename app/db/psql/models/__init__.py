from sqlalchemy.orm import declarative_base

Base = declarative_base()

from .casualties import Casualties
from .target_type import TargetType
from .attack_type import AttackType
from .location import Location
from .event import Event
from .city import City
from .country import Country
from .region import Region
from .terrorist_group import TerroristGroup
