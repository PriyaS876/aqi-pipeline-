"""
test_fetch_data.py

fetch_data.py ke core logic ke liye tests — asli OpenAQ API ko call kiye bina
(requests ko mock karke), taaki tests fast aur reliable rahein.

Chalane ka tareeka:
    pytest -v
"""

import sys
import os
from unittest.mock import patch, MagicMock

# fetch_data.py isi folder mein hai, isliye import path set karo
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fetch_data import find_pm25_sensor_ids, get_pm25_reading


def make_mock_response(json_data, status_code=200):
    """Ek fake requests.Response object banata hai testing ke liye"""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    return mock_resp


class TestFindPm25SensorIds:
    """find_pm25_sensor_ids() function ke tests"""

    @patch("fetch_data.requests.get")
    def test_finds_single_pm25_sensor(self, mock_get):
        """Agar station mein sirf ek PM2.5 sensor hai, to uska ID milna chahiye"""
        mock_get.return_value = make_mock_response({
            "results": [{
                "sensors": [
                    {"id": 111, "parameter": {"name": "pm25"}},
                    {"id": 222, "parameter": {"name": "no2"}},
                ]
            }]
        })

        result = find_pm25_sensor_ids(location_id=17)
        assert result == [111]

    @patch("fetch_data.requests.get")
    def test_finds_multiple_pm25_sensors(self, mock_get):
        """Agar station mein 2 PM2.5 sensors hain (jaise ek dead, ek live),
        dono ke IDs return hone chahiye — yehi humara asli fix tha."""
        mock_get.return_value = make_mock_response({
            "results": [{
                "sensors": [
                    {"id": 35, "parameter": {"name": "pm25"}},
                    {"id": 12234787, "parameter": {"name": "pm25"}},
                ]
            }]
        })

        result = find_pm25_sensor_ids(location_id=17)
        assert set(result) == {35, 12234787}
        assert len(result) == 2

    @patch("fetch_data.requests.get")
    def test_no_pm25_sensor_returns_empty_list(self, mock_get):
        """Agar koi PM2.5 sensor hi nahi hai station mein, khali list aani chahiye"""
        mock_get.return_value = make_mock_response({
            "results": [{
                "sensors": [
                    {"id": 999, "parameter": {"name": "no2"}},
                ]
            }]
        })

        result = find_pm25_sensor_ids(location_id=17)
        assert result == []

    @patch("fetch_data.requests.get")
    def test_api_failure_returns_empty_list(self, mock_get):
        """Agar API error de (jaise 404/500), crash nahi hona chahiye, khali list aani chahiye"""
        mock_get.return_value = make_mock_response({}, status_code=500)

        result = find_pm25_sensor_ids(location_id=17)
        assert result == []


class TestGetPm25Reading:
    """get_pm25_reading() function ke tests — yeh function multiple sensors
    mein se sabse recent wala data chunta hai."""

    @patch("fetch_data.requests.get")
    def test_picks_most_recent_sensor_reading(self, mock_get):
        """Do sensors ke beech, jiska timestamp zyada recent hai wahi return hona chahiye.
        Yeh humara core bug-fix hai — pehle sirf pehla sensor uthaya jaata tha."""

        def side_effect(url, headers):
            if "35" in url:  # purana, dead sensor
                return make_mock_response({
                    "results": [{
                        "value": 999.0,
                        "period": {"datetimeFrom": {"utc": "2018-01-01T00:00:00Z"}}
                    }]
                })
            else:  # naya, live sensor
                return make_mock_response({
                    "results": [{
                        "value": 55.0,
                        "period": {"datetimeFrom": {"utc": "2026-09-12T00:00:00Z"}}
                    }]
                })

        mock_get.side_effect = side_effect

        result = get_pm25_reading(
            location_id=17,
            station_name="R K Puram, Delhi",
            sensor_ids=[35, 12234787]
        )

        assert result is not None
        assert result["value"] == 55.0
        assert result["timestamp_utc"] == "2026-09-12T00:00:00Z"

    @patch("fetch_data.requests.get")
    def test_no_sensors_returns_none(self, mock_get):
        """Khali sensor list di jaaye to None aana chahiye, crash nahi"""
        result = get_pm25_reading(
            location_id=17,
            station_name="Test Station",
            sensor_ids=[]
        )
        assert result is None

    @patch("fetch_data.requests.get")
    def test_anomaly_zero_value_still_returned(self, mock_get):
        """0.0 value bhi ek valid reading maani jaani chahiye (anomaly detection
        Spark mein hoti hai, yahan nahi) — isliye function crash nahi hona chahiye."""
        mock_get.return_value = make_mock_response({
            "results": [{
                "value": 0.0,
                "period": {"datetimeFrom": {"utc": "2026-09-12T00:00:00Z"}}
            }]
        })

        result = get_pm25_reading(
            location_id=301,
            station_name="Vikas Sadan, Gurugram",
            sensor_ids=[14258988]
        )

        assert result is not None
        assert result["value"] == 0.0