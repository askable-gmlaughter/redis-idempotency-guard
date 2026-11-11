from . import config, create_app

create_app().run(host=config.HOST, port=config.PORT, threaded=True)
