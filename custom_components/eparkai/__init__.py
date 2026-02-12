import logging
from datetime import timedelta, datetime
import asyncio
import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import StatisticMetaData, StatisticData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics, statistics_during_period,
)
from homeassistant.const import (
    CONF_ID, CONF_NAME, CONF_USERNAME, CONF_PASSWORD, CONF_CLIENT_ID, UnitOfEnergy
)
from homeassistant.core import HomeAssistant
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

RETRY_LOGIN_DELAY_SEC = 3 * 3600  # 3 val.
RETRY_FETCH_DELAY_SEC = 5 * 3600  # 5 val.

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

    async def async_import_generation(now: datetime, retry_login=False, retry_fetch=False):
        if hass.is_stopping:
            _LOGGER.debug("HA is stopping, skipping generation import")
            return
        try:
            _LOGGER.info(f"Prisijungiama prie eParkai.lt ({now})")
            await hass.async_add_executor_job(client.login)
        except Exception as e:
            _LOGGER.error(f"eParkai login error: {e}")
            if not retry_login:
                _LOGGER.warning("Login nepavyko, bandysiu pakartotinai po 3 val.")
                hass.loop.call_later(RETRY_LOGIN_DELAY_SEC, lambda: asyncio.create_task(async_import_generation(datetime.now(), retry_login=True, retry_fetch=False)))
            else:
                _LOGGER.error("Antras login bandymas į eParkai.lt nesėkmingas – lauksiu sekančios dienos.")
            return
        for power_plant in config[DOMAIN][CONF_POWER_PLANTS]:
            _LOGGER.info(f"Gauta naujinimo užklausa [{power_plant[CONF_NAME]}]")
            try:
                await hass.async_add_executor_job(
                    client.fetch_generation_data,
                    power_plant[CONF_ID],
                    power_plant[CONF_OBJECT_ADDRESS],
                    now
                )
            except Exception as e:
                _LOGGER.error(f"eParkai fetch generation data error [{power_plant[CONF_NAME]}]: {e}")
                if not retry_fetch:
                    _LOGGER.warning(f"Power plant {power_plant[CONF_NAME]}: gauti duomenų nepavyko, bandysiu pakartotinai po 5 val.")
                    hass.loop.call_later(RETRY_FETCH_DELAY_SEC, lambda: asyncio.create_task(async_import_generation(datetime.now(), retry_login=False, retry_fetch=True)))
                else:
                    _LOGGER.error(f"Antras duomenų bandymas nepavyko power plant {power_plant[CONF_NAME]} – lauksiu sekančios dienos.")
                continue
            _LOGGER.info(f"Importuojama statistika/{power_plant[CONF_NAME]}")
            await async_insert_statistics(
                hass,
                power_plant,
                client.get_generation_data(power_plant[CONF_ID])
            )
            _LOGGER.info(f"Import completed for {power_plant[CONF_NAME]}")

    # Tik pagal grafiką -- nebevykdyti po restart
    async_track_time_change(hass, async_import_generation, hour=12, minute=0, second=0)
    return True

async def async_insert_statistics(
    hass: HomeAssistant, power_plant: dict, generation_data: dict
) -> None:
    id_suffix = power_plant[CONF_STATISTICS_ID_SUFFIX] if CONF_STATISTICS_ID_SUFFIX in power_plant else ""
    statistic_id = f"eparkai:energy_generation_{power_plant[CONF_ID]}_{id_suffix}".strip("_")
    _LOGGER.debug(f"Statistic ID for {power_plant[CONF_NAME]} is {statistic_id}")
    if not generation_data:
        _LOGGER.error(f"Received empty generation data for {statistic_id}")
        return None
    _LOGGER.debug(f"Received generation data for {statistic_id}: {generation_data}")
    metadata = StatisticMetaData(
        has_mean=False,
        has_sum=True,
        name=power_plant[CONF_NAME],
        source=DOMAIN,
        statistic_id=statistic_id,
        unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
    )
    _LOGGER.debug(f"Preparing long-term statistics for {statistic_id}")
    statistics = await _async_get_statistics(hass, metadata, power_plant, generation_data)
    _LOGGER.debug(f"Generated statistics for {statistic_id}: {statistics}")
    async_add_external_statistics(hass, metadata, statistics)

async def _async_get_statistics(hass: HomeAssistant, metadata: StatisticMetaData, power_plant: dict, generation_data: dict) -> list[StatisticData]:
    statistic_id = metadata["statistic_id"]
    statistics: list[StatisticData] = []
    generation_percentage = power_plant[CONF_GENERATION_PERCENTAGE]
    sum_ = None
    for ts, generated_kwh in generation_data.items():
        dt_object = datetime.fromtimestamp(ts)
        if generation_percentage != 100:
            generated_percentage_kwh = generated_kwh * (generation_percentage / 100)
            _LOGGER.debug(f"Applying generation percentage of {generation_percentage}% for {statistic_id}: {generated_kwh} kWh -> {generated_percentage_kwh} kWh")
            generated_kwh = generated_percentage_kwh
        if sum_ is None:
            sum_ = await get_yesterday_sum(hass, metadata, dt_object)
        sum_ += generated_kwh
        statistics.append(
            StatisticData(
                start=dt_object.replace(tzinfo=dt_util.get_time_zone("Europe/Vilnius")),
                state=generated_kwh,
                sum=sum_,
            )
        )
    return statistics

async def get_yesterday_sum(hass: HomeAssistant, metadata: StatisticMetaData, date: datetime) -> float:
    statistic_id = metadata["statistic_id"]
    start = date - timedelta(days=1)
    end = date - timedelta(minutes=1)
    _LOGGER.debug(f"Looking history sum for {statistic_id} for {date} between {start} and {end}")
    stat = await get_instance(hass).async_add_executor_job(
        statistics_during_period, hass, start, end, {statistic_id}, "day", None, {"sum"}
    )
    if statistic_id not in stat:
        _LOGGER.debug(f"No history sum found")
        return 0.0
    sum_ = stat[statistic_id][0]["sum"]
    _LOGGER.debug(f"History sum for {statistic_id} = {sum_}")
    return sum_
