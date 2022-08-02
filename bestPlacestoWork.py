import requests
from bs4 import BeautifulSoup as bs
import time, pandas as pd, re, glob
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select

def main():
    url = r'https://en.wikipedia.org/wiki/100_Best_Companies_to_Work_For'
    CDpath = r'C:\Users\flakej\Dropbox\Research\F - Forthcoming\credible_reporting\experimental\chromedriver.exe'
    pre2014 = linksPre2014(url)
    post2014 = linksPost2014(2014,2021)
    df = pd.DataFrame()
    for year in range(2006,2022):
        print(year)
        if year < 2014:
            link = pre2014[year]
            page = requests.get(link)
            dfs = pd.read_html(page.content)
            if year < 2008:
                dfTemp = dfs[4]
            elif year < 2009:
                dfTemp = dfs[6]
            else: 
                dfTemp = dfs[0]
        else:
            link = post2014[year]
            dfTemp = getRankPost2014(CDpath,link)
            if year > 2014:
                dfTemp['RANK'] = dfTemp["RANK\nNAME"].str.extract(r'(\d{1,3})')
                dfTemp['NAME'] = dfTemp["RANK\nNAME"].str.extract(r'\n(\D*)')
                cols = dfTemp.columns.tolist()
                cols = cols[-2:]+cols[1:-2]
                dfTemp = dfTemp[cols]
        dfTemp = standardizeDfs(dfTemp,year)
        df = pd.concat([df,dfTemp])
    df = df.sort_values(['Company','Year'])
    fileName = rf'C:\Users\flakej\Dropbox\Research\F - Forthcoming\credible_reporting\experimental\fortunesBest.csv'
    df.to_csv(fileName,index=False)
    fileName = rf'C:\Users\flakej\Dropbox\Research\F - Forthcoming\credible_reporting\experimental\fortunesBestCompNames.csv'
    compNames = (df.rename(columns={'Company':'FortuneCompany'})).FortuneCompany.drop_duplicates().reset_index(drop=True)
    compNames.to_csv(fileName,index=False)

# Prior to 2014, the ranking tables are in fortunes archives which has a different format from the 
# years after 2014
def linksPost2014(minYear,maxYear):
    post2014 = {year:f'https://fortune.com/best-companies/{year}/search/' for year in range(minYear,maxYear+1)}
    return post2014

def linksPre2014(url):
    page = requests.get(url)
    soup = bs(page.content, "html.parser")
    divs = soup.find(class_='div-col')
    links = {int(link.get_text()): link['href'] for link in divs.find_all('a')}
    links = {key: links[key] for key in links if key < 2014}
    return links

def startSelenium(path,url,options=None):
    options = Options()
    options.headless=False
    driverPath = path
    driver = webdriver.Chrome(options=options, executable_path=driverPath)
    driver.get(url)
    el = Select(driver.find_element_by_xpath("/html/body/div[1]/div/main/div[3]/div[2]/div/div[2]/div/div[2]/span[2]/select"))
    el.select_by_visible_text("100 rows")
    el = driver.find_element_by_xpath("/html/body/div[1]/div/main/div[3]/div[2]/div/div[1]/div[2]")
    rankText = el.text
    el = driver.find_element_by_xpath("/html/body/div[1]/div/main/div[3]/div[2]/div/div[1]/div[1]")
    headerText = el.text
    driver.quit()
    status = "Success"
    return rankText, headerText

def extract_data(headerText,rankText,year):
    #split the text by "REMOVE\n" and then remove any remaining "\n"'s
    header = [h.strip('\n') for h in re.split(r'REMOVE\n',headerText) if len(h) > 0]
    # Other than the column headers and the company names, all other data in table is a number so split the text
    # by the presence of alphabetic characters. Keep only those with non-empty strings
    # Format changed in 2015
    if year == 2014:
        lbPattern = r'([A-Za-z].*)'
    else:
        lbPattern = r'((?:\d{1,3}\n)?[A-Za-z].*)'
    lines = [l for l in re.split(lbPattern,rankText) if len(l) > 0]
    # Create dictionary with company name as key and row values as values. 
    # Output from above creates 2 items for each row so use remainder function to only use every other
    # line item as key and keep the alternating line item as a value - iterate over offsets of the lines array
    # to keep the odd number in the list as the key and the even number in the list as the values
    # Split columns by '\n' 
    d={x: [t for t in y.split('\n') if len(t)>0] for c,(x,y) in enumerate(zip(lines[:-1],lines[1:])) if c%2==0}
    df = pd.DataFrame(d).T.reset_index()
    df.columns=header
    return df

def getRankPost2014(CDpath,link):
    year = int(re.search('\d{4}',link).group())
    rankText, headerText = poll(3,1,1,1.1,startSelenium,CDpath,link)
    df = extract_data(headerText,rankText,year)
    return df

def poll(tries,initialDelay,delay,backoff,function,*args):
    '''
    Following: https://echohack.medium.com/patterns-with-python-poll-an-api-832173a03e93
    '''
    time.sleep(initialDelay)
    for n in range(tries):
        try:
            rankText, headerText = function(*args)
            if headerText is None:
                print(n,delay)
                polling_time = time.strftime("%a, %d %b %Y %H:%M:%S",time.localtime())
                print(f"{polling_time}. Sleeping for {delay} seconds")
                time.sleep(delay)
                delay *= backoff
            else:
                return rankText, headerText
        except Exception as e:
            print(e)
            print(f'Connection Failed after {n} tries')
        raise ConnectionError(f"Failed to poll {function} within {tries} tries.")
            
def standardizeDfs(dfTemp,year):
    cols = dfTemp.columns.tolist()
    # Remove any non-standard characters from column names
    cols = [col.encode('ascii','ignore').decode() for col in cols]
    dfTemp.columns = cols
    dfTemp = dfTemp.rename(columns={'Name':'Company','Company Name':'Company','COMPANY NAME':'Company','RANK':'Rank','NAME':'Company'})
    dfTemp = dfTemp[['Company','Rank']]
    dfTemp['Year'] = year
    return dfTemp

CDpath = r'C:\Users\flakej\Dropbox\Research\F - Forthcoming\credible_reporting\experimental\chromedriver.exe'
main()

