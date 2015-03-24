import java.util.*;

/**
 * Created by Flok on 22.03.15.
 */

public class FlokRLEList<T> implements RLEList<T>{
    // Ой как не хватает X-fast trie.

    private class Pair<T> {
        private int amount;
        private T value;

        public Pair(int amount, T value) {
            this.amount = amount;
            this.value = value;
        }

        public Pair(T value) {
            this(1, value);
        }


        public void incAmount() {
            amount++;
        }

        private void decAmount() {
            amount--;
        }

        private void addAmmount(int delta) {
            amount += delta;
        }

        public int getAmount() {
            return amount;
        }

        public final T getValue() {
            return value;
        }
    }

    private LinkedList<Pair<T>> elems = new LinkedList<>();
    private  Pair<T> lastPair;
    private int size = 0;

    @Override
    public void append(T value) {
        insert(size, value);
    }

    @Override
    public void insert(int index, T value) {
        if(index < 0) {
            throw new IndexOutOfBoundsException("Index less than 0");
        }
        if(index > size) {
            throw new ArrayIndexOutOfBoundsException(String.format("Index is out of bounds. Size: %d, Index: %d", size, index));
        }

        //idx в конце столбца
        if(index == size) {
            if(size == 0 || !Objects.equals(lastPair.getValue(), value)) {
                lastPair = new Pair(value);
                elems.add(lastPair);
            }
            else {
                lastPair.incAmount();
            }
        }
        //idx в начале или середине столбца
        else {
            int totalOffset = 0;
            int curIdx = 0;
            Iterator<Pair<T>> it = elems.iterator();
            Pair<T> prevPair = null;
            while (it.hasNext()) {
                Pair<T> pair = it.next();
                if(totalOffset + pair.getAmount() > index) {
                    // попали на начало/середину интервала значений
                    if(Objects.equals(pair.getValue(), value)) {
                        pair.incAmount();
                        break;
                    }
                    // попали на конец интервала значений
                    if(prevPair != null && Objects.equals(prevPair.getValue(), value) && totalOffset > index) {
                        prevPair.incAmount();
                        break;
                    }
                    // попали в интервал, а в нём значения не совпадают value. разрезаем.
                    it.remove();
                    int idxTempOffset = 0;
                    if(index != totalOffset) { // если мы разрезаем не по левой границе, вставляем левую часть
                        elems.add(curIdx, new Pair<T>(index - totalOffset, pair.getValue()));
                        idxTempOffset++;
                    }
                    elems.add(curIdx + idxTempOffset, new Pair<T>(value));
                    if(index != totalOffset + pair.getAmount()) { // аналогично для разреза не по правой границе.
                        elems.add(curIdx + idxTempOffset + 1, new Pair<T>(pair.getAmount() - index + totalOffset, pair.getValue()));
                    }
                    break;
                }

                totalOffset += pair.getAmount();
                prevPair = pair;
                curIdx++;
            }
        }

        size++;
    }

    @Override
    public T get(int index) {
        Iterator<Pair<T>> it = elems.iterator();
        int totalOffset = 0;
        while(it.hasNext()) {
            Pair<T> pair = it.next();
            totalOffset += pair.getAmount();
            if (totalOffset > index) {
                return pair.getValue();
            }
        }
        throw new ArrayIndexOutOfBoundsException(String.format("Index is out of bounds. Size: %d, Index: %d", size, index));
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<T> it = new Iterator<T>() {
            private int currentIndex = 0;
            private int currentInPairIndex = 0;
            private Pair<T> pair;
            private Stack<Pair<T>> prev = new Stack<>();
            private Iterator<Pair<T>> it = elems.iterator();

            @Override
            public boolean hasNext() {
                return currentIndex < size;
            }

            @Override
            public T next() {
                if(pair == null || currentInPairIndex >= pair.getAmount()) {
                    if(pair != null) {prev.push(pair);}
                    pair = it.next();
                    currentInPairIndex = 0;
                }
                currentInPairIndex++;
                currentIndex++;
                return pair.getValue();
            }

            @Override
            public void remove() {
                pair.decAmount();
                currentInPairIndex--;
                size--;
                if(pair.getAmount() == 0) {
                    it.remove();
                    if(it.hasNext()) {
                        pair = it.next();
                        if(prev.size() != 0 && Objects.equals(prev.peek().getValue(), pair.getValue())) {
                            Pair<T> tempPair = prev.pop();
                            tempPair.addAmmount(pair.getAmount());
                            it.remove();
                            pair = tempPair;
                        }
                    }
                }
            }
        };
        return it;
    }
}
