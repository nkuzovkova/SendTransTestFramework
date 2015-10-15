from selenium import webdriver
import re

def GetOTCFromSandboxAlipay():
    driver = webdriver.Firefox()
    driver.get('https://sandbox.alipaydev.com/sms/outMessageList.htm')
    telephone_field = driver.find_elements_by_id("telephone")[0]
    telephone_field.send_keys("13312020851")
    search_button = driver.find_elements_by_id("telephone")
    text_area = driver.find_element_by_id('mesSearch')
    text_area.click()
    element = driver.find_element_by_xpath('//body/div[2]/div/div[2]/div[2]/table/tbody/tr[1]/td[2]')
    result = re.search("(\d+),", element.text, flags=re.UNICODE)
    driver.quit()
    return result.group(1).encode('utf-8')
    
    