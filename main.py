# coding=utf-8

from json import load
from traceback import format_exc

from models import connect
from controller import listen


if __name__ == "__main__":
    with open("./config.json", "rt") as config_file:
        # Load config
        CONFIG = load(config_file)

    # Connect to database
    connect(**CONFIG)

    # Endless loop
    while True:
        try:
            # Process input
            listen()

        except Exception as error:
            # Failed

            if CONFIG["debug"]:
                # Full traceback
                print(format_exc())

            else:
                # Only error message
                print(f"ERROR: {error}")

        except KeyboardInterrupt:
            # Exit signal
            exit(0)
