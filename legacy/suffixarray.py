from itertools import zip_longest, islice

# - [divsufsort](https://github.com/y-256/libdivsufsort)
#   - it seems this is the best suffix array alg (@2022)
#   - [Dismantling DivSufSort](https://arxiv.org/pdf/1710.01896.pdf)
#   - there is python wrapper, but cannot handle non-ascii char
# - [](http://www.nct9.ne.jp/m_hiroi/light/pyalgo60.html)

# simple suffix array implementation from https://louisabraham.github.io/notebooks/suffix_arrays.html

class SuffixArray:
    def __init__(self, instr):
        self.str = instr
        self.size = len(instr)
        self.rank = self.sa = self.lcp = None

    def reset(self, instr):
        self.__init__(instr)


    def clc_rank_array(self):
        """
        suffix array of s
        O(n * log(n)^2)
        """
        k = 1
        line = to_int_keys(self.str)
        while max(line) < self.size - 1:
            line = to_int_keys([
                a * (self.size + 1) + b + 1 for (a, b) in
                zip_longest(line, islice(line, k, None), fillvalue=-1)])
            k <<= 1
        self.rank = line

    def rank_array(self):
        if self.rank is None:
            self.clc_rank_array()
        return self.rank

    def clc_suffix_array(self):
        self.sa = inverse_array(self.rank_array())

    def suffix_array(self):
        if self.sa is None:
            self.clc_suffix_array()
        return self.sa

    def clc_longest_common_prefix_array(self):
        # refer kasai func in https://github.com/louisabraham/pydivsufsort
        n = self.size
        s = self.str
        rank = self.rank_array()
        sa = self.suffix_array()
        lcp = [0] * n

        k = 0
        for i in range(n):
            if rank[i] == n - 1:
                lcp[n-1] = k = 0
                continue
            j = sa[rank[i] + 1]
            while i + k < n and j + k < n and s[i+k] == s[j+k]:
                k += 1
            lcp[rank[i]] = k
            if k:
                k -= 1
        self.lcp = lcp

    def longest_common_prefix_array(self):
        if self.lcp is None:
            self.clc_longest_common_prefix_array()
        return self.lcp


    def traverse_tree(self, func):
        self.longest_common_prefix_array()
        stack = [(-1, -1)] # (idx, len)
        for i in range(0, self.size):
            stack.append((i, self.size - self.sa[i]))
            hi = self.lcp[i]
            x, h = stack[-1]
            while h > hi:
                func(self.str, self.sa[x], h)
                stack.pop()
                x, h = stack[-1]
            if hi > 0 and h < hi:
                stack.append((i, hi))

    def iter_repeated_substrings(self, key):
        """
        iterates substrings with its repeat count.
        provide key func to filter substrings to return.
        original string, begin index, substr length and count is provided.
        
        note that the substrings are not close or open, i.e. 
        - suffixes of a listed substring are also listed.
        - prefixes of a listed substring are ignored (even if they suffice key).
        - ex. when "hoge" is a candidate, ["oge", "ge", "e"] are also candidates
            and ["hog", "ho", "h"] are not
        """

        self.longest_common_prefix_array()
        stack = [(0, -1, -1)] # (idx, len, count)
        for i in range(self.size):
            stack.append((i, self.size - self.sa[i], 1))
            x, l, c = stack[-1]
            ci, li = (0, self.lcp[i])
            while l > li:
                ci += c
                if key(self.str, self.sa[x], l, ci):
                    yield (self.sa[x], l, ci)
                stack.pop()
                x, l, c = stack[-1]
            if l == li:
                stack[-1] = (x, l, c + ci)
            elif li > 0 and l < li:
                stack.append((i, li, ci))
        return


def to_int_keys(l):
    """
    l: iterable of keys
    returns: a list with integer keys
    """
    seen = set()
    ls = []
    for e in l:
        if not e in seen:
            ls.append(e)
            seen.add(e)
    ls.sort()
    index = {v: i for i, v in enumerate(ls)}
    return [index[v] for v in l]


def inverse_array(l):
    n = len(l)
    ans = [0] * n
    for i in range(n):
        ans[l[i]] = i
    return ans
