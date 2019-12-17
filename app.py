import tornado.web
import tornado.locks
import tornado.ioloop

from api import BeersView


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/beers", BeersView.BeerHandler),
            (r"/report", BeersView.ReportHandler)
        ]
        super(Application, self).__init__(handlers)


async def main():
    app = Application()
    app.listen(3000)

    shutdown_event = tornado.locks.Event()
    await shutdown_event.wait()


if __name__ == "__main__":
    tornado.ioloop.IOLoop.current().run_sync(main)
