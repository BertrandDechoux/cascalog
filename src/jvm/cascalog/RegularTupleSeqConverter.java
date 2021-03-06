/*
    Copyright 2010 Nathan Marz
 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cascalog;

import cascading.tuple.Tuple;
import clojure.lang.ISeq;

import java.util.Iterator;

public class RegularTupleSeqConverter implements Iterator<ISeq> {
    private Iterator<Tuple> _tuples;

    public RegularTupleSeqConverter(Iterator<Tuple> tuples) {
        _tuples = tuples;
    }

    public boolean hasNext() {
        return _tuples.hasNext();
    }

    public ISeq next() {
        return Util.coerceFromTuple(_tuples.next());
    }

    public void remove() {
        _tuples.remove();
    }
}