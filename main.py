from collections.abc import Iterable
from datetime import datetime
import json
import os
from pprint import pprint
from random import random, randint
import re

from dotenv import load_dotenv

from pymongo import MongoClient

OPERATORS = {'+': (1, lambda x, y: '{'+f'"$sum": [{x}, {y}]' + '}'),
             '-': (1, lambda x, y: '{'+f'"$subtract": [{x}, {y}]' + '}'),
             '*': (2, lambda x, y: '{'+f'"$multiply": [{x}, {y}]' + '}'),
             '/': (2, lambda x, y: '{'+f'"$divide": [{x}, {y}]' + '}')}

json_query = '{"group": ["$date"], ' \
             '"filters": ["$opportunity < 1000000", "$opportunity > 2000", "$state $eq success"],' \
             '"calculate": "($opportunity + $deal_price) * $count"}'


def main():
    env = os.path.join(os.path.abspath(os.curdir), '.env')
    load_dotenv(env)

    mongo_url = os.getenv('MONGO_URL')
    mongo_client = MongoClient(mongo_url)
    db = mongo_client.sales
    collection = db['deals']

    result = list(collection.aggregate(pipeline_generator(json_query)))
    pprint(result)


def parse_query_option(options: str) -> list:
    compare = {'<': '$lt', '<=': '$lte', '>': '$gt', '>=': '$gte', '$eq': '$eq', '=': '$eq'}
    return list(compare[option] if option in compare else option for option in options.split())


def exp_generator(source_exp: str) -> str:
    def polski_calc(sorted_expression: Iterable) -> str:
        calc_expression = []
        for sorted_element in sorted_expression:
            if sorted_element in OPERATORS:
                operand2, operand1 = calc_expression.pop(), calc_expression.pop()
                if len(operand1.split()) == 1:
                    operand1 = '"' + operand1 + '"'
                if len(operand2.split()) == 1:
                    operand2 = '"' + operand2 + '"'
                calc_expression.append(OPERATORS[sorted_element][1](operand1, operand2))
            else:
                calc_expression.append(sorted_element)
        return calc_expression[0]

    def operand_sorter(parsed_formula: list) -> Iterable:
        stack = []
        for token in parsed_formula:
            if token in OPERATORS:
                while stack and stack[-1] != '(' and OPERATORS[token][0] <= OPERATORS[stack[-1]][0]:
                    yield stack.pop()
                stack.append(token)
            elif token == ')':
                while stack:
                    x = stack.pop()
                    if x == '(':
                        break
                    yield x
            elif token == '(':
                stack.append(token)
            else:
                yield token
        while stack:
            yield stack.pop()

    def math_exp_parser(expression_str: str) -> list:
        res = re.findall(r'\$\w*|\S?', expression_str)
        res = [elem for elem in res if elem]
        return res

    return polski_calc(operand_sorter(math_exp_parser(source_exp)))


def pipeline_generator(query_str: str) -> list:
    pipeline = []
    query = json.loads(query_str)
    group_field = ''
    if query['filters'] is not None:
        pipeline.append({'$match': {}})
        if len(query['filters']) > 1:
            pipeline[0]["$match"]["$and"] = list()
            for query_filter in query['filters']:
                filter_operator, filter_condition, filter_value = parse_query_option(query_filter)
                try:
                    filter_value = float(filter_value)
                except ValueError:
                    pass
                if filter_condition == "$eq":
                    pipeline[0]["$match"]["$and"].append({filter_operator[1:]: filter_value})
                else:
                    pipeline[0]["$match"]["$and"].append({filter_operator[1:]: {filter_condition: filter_value}})
        else:
            filter_operator, filter_condition, filter_value = parse_query_option(query['filters'][0])
            try:
                filter_value = float(filter_value)
            except ValueError:
                pass
            if filter_condition == "$eq":
                pipeline[0]["$match"] = {filter_operator[1:]: filter_value}
            else:
                pipeline[0]["$match"] = {filter_operator[1:]: {filter_condition: filter_value}}
    if query['group'] is not None:
        group_field = parse_query_option(query['group'][0])
        if group_field == '$date':
            group_exp = {'$dateToString': {'format': '%d.%m.%Y', 'date': group_field}}
        else:
            group_exp = group_field
        pipeline.append({'$group': {}})
        pipeline[1]['$group']['_id'] = group_exp
        pipeline[1]['$group']['count'] = {'$sum': 1}
        calc_members = re.findall(r'\$\w+', query['calculate'])
        for member in calc_members:
            if member != '$count':
                pipeline[1]['$group'][member[1:]] = {'$sum': member}

        pipeline.append({'$sort': {'_id': 1}})

    if query['calculate'] is not None:
        if isinstance(group_field, list) and group_field[0]:
            pipeline.append({'$project': {group_field[0][1:]: '$_id', '_id': 0}})
            pipeline[3]['$project']['calculate'] = json.loads(exp_generator(query['calculate']))
    return pipeline


def data_generator(object_count: int = 100) -> list:
    collection = []
    for i in range(object_count):
        start_date = datetime(2021, 10, randint(1, 31))
        opportunity = round(randint(100000, 1000000) * random(), 2)
        state = ['fail', 'success'][randint(0, 1)]
        sum_data = round(randint(10000, 100000) * random(), 2)
        collection.append({'date': start_date, 'opportunity': opportunity, 'state': state, 'deal_price': sum_data})
    return collection


if __name__ == '__main__':
    main()
