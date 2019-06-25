import unittest

import pandas as pd
from pathlib import Path
from pykakasi import kakasi
from tqdm import tqdm

import zip2weather


class TestZip2Weather(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_zip2pref(self):
        kakasi_ = kakasi()
        kakasi_.setMode('H', 'a')
        kakasi_.setMode('K', 'a')
        kakasi_.setMode('J', 'a')
        conv = kakasi_.getConverter()

        romaji_converter = {'hokkaido': 'hokkaidou', 'tokyo': 'toukyouto', 'kyoto': 'kyoutofu', 'osaka': 'oosakafu',
                            'hyogo': 'hyougoken', 'kagoshima': 'shikajishimaken', 'hiroshima': 'koushimaken',
                            'oita': 'ooitaken', 'kochi': 'kouchiken'}

        if not Path('pref_test.csv').is_file():
            self.make_pref_test_case()
        test_pattern = pd.read_csv('pref_test.csv')

        for i, test_case in tqdm(test_pattern.iterrows()):
            self.assertEqual(romaji_converter.get(test_case['pref'], test_case['pref'] + 'ken'),
                             conv.do(zip2weather.zip2pref(test_case['postal'])))

    def make_pref_test_case(self):
        df = pd.read_excel('Book1.xlsx')
        unique_zip = df['zip'].unique()
        pref_list = []
        for postal in tqdm(unique_zip):
            pref_list.append(df[df['zip'] == postal]['prefecture'].values[0])
        pd.DataFrame(list(zip(unique_zip, pref_list)), columns=['postal', 'pref']
                     ).to_csv('pref_test.csv', index=False)


if __name__ == '__main__':
    unittest.main()