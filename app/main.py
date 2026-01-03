import uvicorn

from xraymgr.schema import init_db_schema
from xraymgr.web import get_app


def main():
    # init DB schema
    init_db_schema()
    print("Database schema initialized / verified.")

    app = get_app()

    # اجرای uvicorn روی 0.0.0.0:8000
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
