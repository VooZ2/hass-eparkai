import logging
from datetime import timedelta, datetime
import asyncio
import voluptuous as vol

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticMetaData, 
    StatisticData,
    StatisticMeanType
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics, 
    statistics_during_period,
)
from homeassistant.const import (
    CONF_ID, CONF_NAME, CONF_USERNAME, CONF_PASSWORD, CONF_CLIENT_ID, 
    UnitOfEnergy, EVENT_HOMEASSISTANT_STARTED
)
import homeassistant.helpers.config_validation as cv
from homeassistant.core import HomeAssistant, Event
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.typing import ConfigType
from homeassistant.util import dt as dt_util
from .eparkai_client import EParkaiClient

_LOGGER = logging.getLogger(__name__)

DOMAIN = "eparkai"
CONF_POWER_PLANTS = "power_plants"
CONF_OBJECT_ADDRESS = "object_address"
CONF_GENERATION_PERCENTAGE = "generation_percentage"
CONF_STATISTICS_ID_SUFFIX = "statistics_id_suffix"

RETRY_DELAY_SEC = 3 * 3600  # 3 valandos

POWER_PLANT_SCHEMA = vol.Schema({
    vol.Required(CONF_NAME): cv.string,
    vol.Required(CONF_ID): cv.string,
    vol.Optional(CONF_OBJECT_ADDRESS, default=None): vol.Any(None, cv.string),
    vol.Optional(CONF_STATISTICS_ID_SUFFIX, default=""): cv.string,
    vol.Optional(CONF_GENERATION_PERCENTAGE, default=100): vol.All(int, vol.Range(min=1, max=100))
})

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required(CONF_USERNAME): cv.string,
        vol.Required(CONF_PASSWORD): cv.string,
        vol.Required(CONF_CLIENT_ID): cv.string,
        vol.Required(CONF_POWER_PLANTS): cv.ensure_list(POWER_PLANT_SCHEMA),
    })
}, extra=vol.ALLOW_EXTRA)

async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    if DOMAIN not in config:
        return True
    
    hass.data.setdefault(DOMAIN, config[DOMAIN])
    client = EParkaiClient(
        username=config[DOMAIN][CONF_USERNAME],
        password=config[DOMAIN][CONF_PASSWORD],
        client_id=config[DOMAIN][CONF_CLIENT_ID]
    )

    async def async_import_generation(now: datetime, retry_count=0):
        if hass.is_stopping:
            return

        try:
            _LOGGER.info(f"eParkai.lt: Pradedamas duomenų importas")
            await hass.async_add_executor_job(client.login)
            
            for power_plant in config[DOMAIN][CONF_POWER_PLANTS]:
                _LOGGER.info(f"eParkai: Siunčiami duomenys [{power_plant[CONF_NAME]}]")
                # Ir praėjusi savaitė: kitaip dienos, praleistos per savaitės
                # sandūrą ar HA/serverio neveikimą, niekada nebeatsisiunčiamos.
                for days_back in (7, 0):
                    await hass.async_add_executor_job(
                        client.fetch_generation_data,
                        power_plant[CONF_ID],
                        power_plant[CONF_OBJECT_ADDRESS],
                        now - timedelta(days=days_back)
                    )
                
                await async_insert_statistics(
                    hass,
                    power_plant,
                    client.get_generation_data(power_plant[CONF_ID])
                )
            _LOGGER.info("eParkai.lt: Importas baigtas")

        except Exception as e:
            _LOGGER.error(f"eParkai klaida: {e}")
            if retry_count < 3:
                new_retry = retry_count + 1
                _LOGGER.warning(f"eParkai: Bandymas {new_retry}/3 nepavyko. Bandysiu po 3 val.")
                hass.loop.call_later(
                    RETRY_DELAY_SEC, 
                    lambda: asyncio.create_task(async_import_generation(dt_util.now(), retry_count=new_retry))
                )

    # 10:00 val. grafikas
    async_track_time_change(hass, async_import_generation, hour=10, minute=0, second=0)

    # Importas po restarto
    async def _started_import(event: Event) -> None:
        await asyncio.sleep(60)
        _LOGGER.info("HA startavo: eParkai duomenų importas po restarto")
        await async_import_generation(dt_util.now())

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _started_import)
    return True

async def async_insert_statistics(hass: HomeAssistant, power_plant: dict, generation_data: dict) -> None:
    if not generation_data:
        return

    suffix = power_plant.get(CONF_STATISTICS_ID_SUFFIX, "")
    statistic_id = f"eparkai:energy_generation_{power_plant[CONF_ID]}_{suffix}".strip("_")

    metadata = StatisticMetaData(
        has_mean=False,
        mean_type=StatisticMeanType.NONE, # Kritinis pataisymas
        has_sum=True,
        name=power_plant[CONF_NAME],
        source=DOMAIN,
        statistic_id=statistic_id,
        unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
    )

    statistics = await _async_get_statistics(hass, statistic_id, power_plant, generation_data)
    async_add_external_statistics(hass, metadata, statistics)

async def _async_get_statistics(hass: HomeAssistant, statistic_id: str, power_plant: dict, generation_data: dict) -> list[StatisticData]:
    statistics: list[StatisticData] = []
    generation_percentage = power_plant[CONF_GENERATION_PERCENTAGE]
    sum_ = None

    for ts, generated_kwh in sorted(generation_data.items()):
        dt_object = datetime.fromtimestamp(ts, tz=dt_util.get_time_zone("Europe/Vilnius"))
        
        if generation_percentage != 100:
            generated_kwh = generated_kwh * (generation_percentage / 100)
            
        if sum_ is None:
            sum_ = await get_previous_sum(hass, statistic_id, dt_object)
        
        sum_ += generated_kwh
        statistics.append(StatisticData(start=dt_object, state=generated_kwh, sum=sum_))
    return statistics

def _row_start_ts(start) -> float:
    return start.timestamp() if isinstance(start, datetime) else float(start)


async def get_previous_sum(hass: HomeAssistant, statistic_id: str, date: datetime) -> float:
    # Suma paskutinės valandos griežtai PRIEŠ pirmą importuojamą valandą.
    # Anksčiau naudotas "day" periodas grąžindavo ir pačios importo pradžios dienos
    # sumą, todėl tos dienos gamyba būdavo pridėta antrą kartą (šuolis 00:00 valandą).
    limit_ts = date.timestamp()
    for lookback, period in ((timedelta(days=31), "hour"), (timedelta(days=1100), "month")):
        stat = await get_instance(hass).async_add_executor_job(
            statistics_during_period, hass, date - lookback, date, {statistic_id}, period, None, {"sum"}
        )
        rows = stat.get(statistic_id) or []
        if period == "hour":
            rows = [r for r in rows if _row_start_ts(r["start"]) < limit_ts]
        else:
            # Mėnesio eilutę imame tik jei visas mėnuo baigėsi iki importo pradžios
            rows = [r for r in rows if _row_start_ts(r["end"]) <= limit_ts]
        if rows:
            return rows[-1].get("sum") or 0.0
    return 0.0
