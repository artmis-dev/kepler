import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class PokeApiClient:
    """Cliente para interação com a PokeAPI."""

    BASE_URL = "https://pokeapi.co/api"
    VERSION = "/v2"

    def _get(self, endpoint: str, params: dict | None = None) -> dict | list:
        """Executa uma requisição GET e retorna o JSON da resposta."""
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            logger.error("Falha na requisição para %s: %s", endpoint, error)
            return {}

    def fetch_all_pokemons(self, limit: int = 60, offset: int = 0) -> list[dict]:
        """Busca uma lista de pokémons da PokeAPI."""
        endpoint = f"{self.BASE_URL}{self.VERSION}/pokemon/"
        params = {"limit": limit, "offset": offset}
        result = self._get(endpoint, params=params)
        return result.get("results", [])

    def fetch_one_pokemon(self, name: str) -> dict[str, Any]:
        """Busca as informações de um pokémon pelo nome."""
        endpoint = f"{self.BASE_URL}{self.VERSION}/pokemon/{name}"
        return self._get(endpoint)
