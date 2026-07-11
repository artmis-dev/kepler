from src.models.PokeApiClient import PokeApiClient


def main():
    client = PokeApiClient()

    pokemon = client.fetch_all_pokemons()
    print(pokemon)

    details_poke = client.fetch_one_pokemon("bulbasaur")
    print(details_poke)


if __name__ == "__main__":
    main()
