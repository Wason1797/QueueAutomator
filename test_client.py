import httpx
import asyncio


async def send_data(client: httpx.AsyncClient, data: int) -> httpx.Response:
    return await client.post(f'http://localhost:8000/process/{data}')


async def main() -> None:
    async with httpx.AsyncClient() as client:
        coros = [send_data(client, data) for data in range(1000)]
        await asyncio.gather(*coros)
        return None


if __name__ == '__main__':
    asyncio.run(main())
