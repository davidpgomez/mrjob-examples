#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 12:20:21 2019

@author: maria.garcia
"""

from mrjob.job import MRJob
from mrjob.job import MRStep


class MRArista(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]

    # A,B -> A,(A,B) and B,(A,B)
    def mapper(self, _, arista):
        a, b = arista.split(',')
        if a != b:
            yield a, (a,b)
            yield b, (a,b)

    # (A, [AB, AC, AD]) -> AB,A,3 and AC,A,3 and AD,A,3
    def reducer1(self, key, values):
        # As 'values' is a generator (remember, is created using 'yield' in the mapper step)
        # and we need its length, we transform values in a list, resulting in values_as_list
        values_as_list = list(values)
        new_value = (key, len(values_as_list))
        for v in values_as_list:
            yield v, new_value

    # (AB, [(A,3),(B,2)]) -> AB, (3,2)
    def reducer2(self, key, values):
        # We use values_as_list for the same reason as in reducer1
        value_as_list = list(values)
        origin, target = value_as_list[0], value_as_list[1]
        # TODO we should check the order of the edges, as it is now could lead to arity swap
        yield key, (origin[1], target[1])

if __name__ == '__main__':
    MRArista.run()

