import asyncio
from aiohttp import web
from web.handlers import bili

"""
from config.log4 import web_access_logger

@web.middleware
async def log_access(request, handler):
    ua = request.headers.get("User-Agent", "NON_UA")
    resp = await handler(request)
    web_access_logger.info(f"{request.method}-{resp.status} {request.remote} {request.url}\n\t{ua}")
    return resp
"""


async def main():
    app = web.Application()
    app.add_routes([
        web.get('/bili/broadcast', bili.broadcast),
        web.get('/bili/guards', bili.guards),
        web.get('/bili/raffles', bili.raffles),
        web.get('/bili/query_raffles_by_user', bili.query_raffles_by_user),
    ])
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, '127.0.0.1', 2048)
    await site.start()
    print("Site started.")

    while True:
        await asyncio.sleep(100)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
