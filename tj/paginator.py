"""
Django pagination doesn't work for pandas objects,
and a customizable solution is perhaps nicer anyhow.
"""
import math


class PandasPage(object):
    """
    Represents a given "display page" of pandas data
    """
    def __init__(self, data_frame, page_number, start_index, end_index, num_items):
        self.data_frame = data_frame
        self.page_number = page_number
        self.start_index = start_index
        self.end_index = end_index
        self.num_items = num_items

        self.prev_page_number = page_number - 1
        self.next_page_number = page_number + 1

        self.has_prev_page = start_index > 1
        self.has_next_page = end_index < num_items


class PandasPaginator(object):
    """
    Splits pandas data_frames up into display pages.
    """
    def __init__(self, data_frame, count_per_page):
        self.count_per_page = count_per_page
        self.data_frame = data_frame
        self.num_items = len(data_frame)
        self.num_pages = math.ceil(self.num_items/float(count_per_page))

    def get_page(self, page_number):
        if self.data_frame.empty:
            return PandasPage(self.data_frame, 0, 0, 0, 0)

        start_index = page_number * self.count_per_page + 1
        end_index = min(start_index + self.count_per_page - 1, self.num_items)
        data_slice = self.data_frame.iloc[start_index - 1:end_index]
        return PandasPage(data_slice, page_number, start_index, end_index, self.num_items)
