# run_report.py
import asyncio
from reports import send_report

async def main():
    # Можно запустить любой отчёт: 'утро', 'обед', 'вечер'
    await send_report('утро', 18)

if __name__ == "__main__":
    asyncio.run(main())
