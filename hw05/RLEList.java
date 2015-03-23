import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface RLEList<T> {
    void append(T value);
    void insert(int index, T value);
    T get(int index);
    Iterator<T> iterator();

    class Demo {

        public static void main(String[] args) {
            RLEList<Integer> list = new RLEListRefImpl<Integer>();

            list.append(1);
            list.append(2);
            list.append(2);
            list.append(2);
            list.append(2);
            list.append(2);
            list.append(3);
            list.append(4);
            list.append(4);

            assertEquals(list.iterator(), new Integer[] {1, 2, 2, 2, 2, 2, 3, 4, 4});

            list.insert(3, 6);
            assertEquals(list.iterator(), new Integer[] {1, 2, 2, 6, 2, 2, 2, 3, 4, 4});


            Iterator<Integer> i = list.iterator();
            i.next();
            i.next();
            i.next();
            i.next(); // return number 6. This one should be deleted.
            i.remove();

            assertEquals(list.iterator(), new Integer[] {1, 2, 2, 2, 2, 2, 3, 4, 4});
        }

        private static void assertEquals(Iterator iterator, Object[] toCheck) {
            int position = 0;
            while (iterator.hasNext()) {
                if (!toCheck[position++].equals(iterator.next())) {
                    throw new IllegalStateException("Assertion failed!");
                }
            }
        }

    }

}

class RLEListRefImpl<T> implements RLEList<T> {

    private final List<Pair<T>> storage = new ArrayList<Pair<T>>();

    public void append(T value) {
        int last = storage.size() - 1;
        if (last == -1 || !storage.get(last).value.equals(value)) {
            storage.add(new Pair<T>(value));

        } else {
            storage.get(last).count++;

        }
    }

    public void insert(int index, T value) {

        int indexInStorage = getRealIndex(index);

        // that is quiet ineffective
        // but code becomes much easier to understood
        int maxFakeIndex = getMaxFakeIndex(indexInStorage);

        Pair<T> pairAtIndex = storage.get(indexInStorage);
        if (pairAtIndex.value.equals(value)) {
            pairAtIndex.count++;
        } else {

            // there could be the case:
            //   raw      |   encoded
            // ===========|=======================
            // 11111      |   (5)->1
            // { insert value = 2 in position = 3 }
            // 111211     |   (3)->1 (1)->2 (2)->1
            //
            // The following code solve such situations

            int rightPartCount = maxFakeIndex - index;

            storage.get(indexInStorage).count -= rightPartCount;

            // add new value in the middle
            storage.add(indexInStorage + 1, new Pair<T>(value));

            if (rightPartCount > 0) {
                storage.add(indexInStorage + 2, new Pair<T>(storage.get(indexInStorage).value, rightPartCount));
            }

            if (storage.get(indexInStorage).count == 0) {
                storage.remove(indexInStorage);
            }

        }

    }

    public T get(int index) {
        return storage.get(getRealIndex(index)).value;
    }

    public Iterator<T> iterator() {
        return new RLEListIterator();
    }

    private int getRealIndex(int index) {
        int counter = 0;
        int indexInStorage = 0;
        for (Pair<T> pair : storage) {
            counter += pair.count;
            if (index < counter) {
                return indexInStorage;
            }

            indexInStorage++;
        }

        throw new IndexOutOfBoundsException("No value for index " + index);
    }

    private int getMaxFakeIndex(int realIndex) {
        int summary = 0;
        for (int i = 0; i <= realIndex; i++) {
            summary += storage.get(i).count;
        }

        return summary;
    }

    private static class Pair<T> {

        public T value;

        public int count = 1;

        public Pair(T value) {
            this.value = value;
        }

        public Pair(T value, int count) {
            this.value = value;
            this.count = count;
        }

    }

    private class RLEListIterator implements Iterator<T> {

        private int slotPosition = 0;

        private int storagePosition = 0;

        @Override
        public boolean hasNext() {
            int last = storage.size() - 1;
            if (last == -1 || (storagePosition == last && storage.get(storagePosition).count == slotPosition)) {
                return false;
            }

            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new IllegalStateException("Have no more elements!");
            }

            if (storage.get(storagePosition).count == slotPosition) {
                slotPosition = 0;
                storagePosition++;
            }

            slotPosition++;
            return storage.get(storagePosition).value;
        }

        @Override
        public void remove() {
            int toRemoveStoragePosition = slotPosition - 1 < 0 ? storagePosition - 1 : storagePosition;
            if (storage.get(toRemoveStoragePosition).count - 1 == 0) {

                storage.remove(toRemoveStoragePosition);

                // check weather we need to merge
                if (storage.get(toRemoveStoragePosition - 1).value.equals(storage.get(toRemoveStoragePosition).value)) {
                    storage.get(toRemoveStoragePosition - 1).count += storage.get(toRemoveStoragePosition).count;
                    storage.remove(toRemoveStoragePosition);
                }

            } else {
                storage.get(toRemoveStoragePosition).count--;
            }
        }

    }

}
