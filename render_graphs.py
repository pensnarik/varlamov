#!/usr/bin/env python

import os
import sys
import psycopg2, psycopg2.extras

class App(object):

    year_range = (2008, 2018,)

    def __init__(self):
        self.conn = psycopg2.connect('postgresql://postgres@127.0.0.1:20000/database')

    def get_maximum(self, data):
        max = 0
        for row in data:
            for year in range(self.year_range[0], self.year_range[1]):
                if row[str(year)] > max:
                    max = row[str(year)]
        return max

    def get_table_def(self, data, title_column):
        max = self.get_maximum(data)

        table_def = '<table cellpadding="4" cellspacing="0" border="1">'

        table_def = table_def + '\n<tr><td>ISO / year</td>'

        for year in range(self.year_range[0], self.year_range[1]):
            table_def = table_def + '<td>%s</td>' % year

        table_def = table_def + '</tr>'

        for row in data:
            table_def = table_def + '\n<tr><td>%s</td>' % row[title_column]
            for year in range(self.year_range[0], self.year_range[1]):
                value1 = '%02x' % (255 - row[str(year)] / float(max) * 250)
                value2 = '%02x' % (row[str(year)] / float(max) * 250)
                color = '30%s%s' % (value1, value2)
                table_def = table_def + '<td bgcolor="#%s">%s</td>' % (color, row[str(year)])
            table_def = table_def + '</tr>\n'

        table_def = table_def + '\n</table>'

        return table_def

    def get_data(self, query):
        result = list()

        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)

        for row in cursor.fetchall():
            result.append(dict(row))

        cursor.close()

        return result

    def render(self, query, title_column):
        template = '''
        graph G {
        a0 [fontname="Hack" fontsize=14 shape=box label =<\n%s>];
        }
        '''
        data = self.get_data(query)
        return template % self.get_table_def(data, title_column)

    def run(self, argv):

        with open('iso.dot', 'wt') as f:
            f.write(self.render('select * from iso_stat', 'exif_iso'))

        os.system('dot -Tpng iso.dot -o iso.png')

        with open('cameras.dot', 'wt') as f:
            f.write(self.render('select * from cameras_stat', 'exif_camera_model'))

        os.system('dot -Tpng cameras.dot -o cameras.png')

        with open('posts.dot', 'wt') as f:
            f.write(self.render('select * from posts_stat', 'metric'))

        os.system('dot -Tpng posts.dot -o posts.png')


        self.conn.close()

if __name__ == '__main__':
    app = App()
    sys.exit(app.run(sys.argv))
