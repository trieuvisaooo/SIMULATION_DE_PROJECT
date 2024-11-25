import requests  # Dùng để lấy tỷ giá từ API

# Cập nhật tỷ giá USD sang VND từ API - exchangerate-api
def get_exchange_rate():
    try:
        response = requests.get('https://v6.exchangerate-api.com/v6/a6b76e448f3615e72acc18ba/latest/USD')
        data = response.json()
        return data['conversion_rates']['VND']
    except Exception as e:
        print(f"Error getting exchange rate: {e}")
        return 23000  # Tỷ giá cố định nếu không lấy được từ API