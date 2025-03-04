import requests
import time
from urllib.parse import urlparse

# Step 1: Get the final redirected URL with sleep
short_url = 'https://maps.app.goo.gl/EVF27mgTRcxMfbZu7'

# Use GET instead of HEAD and add sleep
response = requests.get(short_url, allow_redirects=True)
time.sleep(10)  # Wait for 10 seconds to ensure redirection is complete

# Step 2: Extract the redirected URL
redirected_url = response.url


print(f"Original URL: {short_url}")

print(f"Final Redirected URL: {redirected_url}")
