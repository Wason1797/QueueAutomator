import httpx
import asyncio


async def send_data(client: httpx.AsyncClient, data: str) -> httpx.Response:
    return await client.post(f'http://localhost:8000/process/queue/{data}')


async def main() -> None:
    async with httpx.AsyncClient() as client:
        coros = [send_data(client, f'process_{data}') for data in range(10)]
        await asyncio.gather(*coros)
        return None


if __name__ == '__main__':
    asyncio.run(main())
