import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public interface RLEList<T> {
    void append(T value);

    void insert(int index, T value);

    T get(int index);

    Iterator<T> iterator();

}

class RLEListRefImpl<T> implements RLEList<T> {

    private Integer MIN_INDEX = -1;
    private Integer MAX_INDEX = Integer.MAX_VALUE;

    private Section MIN_STUB = new Section(null, MIN_INDEX, 0);
    private Section MAX_STUB = new Section(null, MAX_INDEX, 0);

    private final TreeMap<Integer, Section> map = new TreeMap<>();

    {
        MIN_STUB.right = MAX_STUB;
        MAX_STUB.left = MIN_STUB;
        putSection(MIN_STUB);
        putSection(MAX_STUB);
    }

    private class Section {
        private T value;
        private int index;
        private int copies;
        private Section left;
        private Section right;

        Section(final T value, final int index, final int copies) {
            this.value = value;
            this.index = index;
            this.copies = copies;
        }

        public boolean inRightBound(int testIndex) {
            return testIndex < index + copies;
        }

        public boolean isLeftBorder(int testIndex) {
            return index == testIndex;
        }

        public boolean isRightBorder(int testIndex) {
            return index + copies - 1 == testIndex;
        }

        public boolean isSingle() {
            return copies == 1;
        }

        public boolean isNextToLeft(final int testIndex) {
            return (testIndex + 1) == index;
        }

        public boolean isNextToRight(final int testIndex) {
            return testIndex == index + copies;
        }
    }

    public T get(int index) {
        checkIndex(index);
        Section section = map.floorEntry(index).getValue();
        if (section.inRightBound(index)) {
            return section.value;
        } else {
            return null;
        }
    }

    public Iterator<T> iterator() {
        return new Iterator<T>() {
            Section current = MIN_STUB;
            int counter = 0;

            @Override
            public boolean hasNext() {
                return counter != 0 || current.right != MAX_STUB;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (counter == 0) {
                    current = current.right;
                    counter = current.copies;
                }
                counter--;
                return current.value;
            }

            @Override
            public void remove() {
                if(current.copies > 1){
                    current.copies--;
                } else {
                    connectPair(current.left,current.right);
                    removeSection(current);
                }
            }
        };
    }

    public void append(T value) {
        // Добавлятьбудем к самому правому
        Section section = MAX_STUB.left;
        // Если слева что то есть и имеет такое же значение
        if (section != MIN_STUB && section.value.equals(value)) {
            appendOneFromRight(section);
        } else {
            // Слева значение неравное(или пустота) справа пустота
            int newIndex = section.index + 1;
            checkIndex(newIndex);
            Section newSection = new Section(value, newIndex, 1);
            connectTriple(section, newSection, MAX_STUB);
            putSection(newSection);
        }
    }

    public void insert(int index, T value) {
        checkIndex(index);
        // Поищем что нибудь левее либо равное
        Section section = map.floorEntry(index).getValue();
        // Случай когда всталвяем слева и левее ничего нет
        if (section == MIN_STUB) {
            Section rightNeighbour = section.right;
            // Справа тоже ничего нет
            if (rightNeighbour == MAX_STUB) {
                Section newSection = new Section(value, index, 1);
                connectTriple(section, newSection, rightNeighbour);
                putSection(newSection);
            } else {
                // Справа на соседнем индексе нашли такой же объект, сливаемся с ним
                if (rightNeighbour.isNextToLeft(index) && value.equals(rightNeighbour.value)) {
                    appendOneFromLeft(rightNeighbour);
                } else {
                    // Слева пустота справа другой объект и пустой промежуток- вставляем новый
                    Section newSection = new Section(value, index, 1);
                    putSection(newSection);
                }
            }
        } else {
            // Попали ли в диапазон левого ?
            // Тоесть будем ли вставляь в уже существующий объект.
            if (section.inRightBound(index)) {
                // Если значения равны то изменения не трбуются
                if (section.value == value) {
                    return;
                } else {
                    // Если тут всего один элемент, просто заменим его
                    if (section.isSingle()) {
                        section.value = value;
                    }
                    // Случай когда индeкc точно равен левой границе подинтервала
                    else if (section.isLeftBorder(index)) {
                        // Интервал разделяется на два куска, левый меняет значение правй добавляется
                        int newIndex = index + 1;
                        checkIndex(newIndex);
                        Section rightPart =
                                new Section(section.value, newIndex, section.copies - 1);
                        Section leftNeighbour = section.left;
                        Section rightNeighbour = section.right;
                        // Если слева что то есть, то может быть с ним можно слится
                        if (leftNeighbour != MIN_STUB && leftNeighbour.value.equals(value)) {
                            appendOneFromRight(leftNeighbour);
                            removeSection(section);
                            connectTriple(leftNeighbour, rightPart, rightNeighbour);
                        } else {
                            section.copies = 1;
                            section.value = value;
                            connectTriple(section, rightPart, rightNeighbour);
                        }
                        putSection(rightPart);
                    }
                    // Случай когда индeкc точно равен правой границе подинтервала
                    else if (section.isRightBorder(index)) {
                        // Интервал делится на два куска слева со старым значением справа добавляем с новым
                        Section rightNeighbour = section.right;
                        section.copies--;
                        // Если справа что то есть, то может быть с ним можно слится
                        if (rightNeighbour != MAX_STUB && rightNeighbour.value.equals(value)) {
                            appendOneFromLeft(rightNeighbour);
                        } else {
                            Section rightPart = new Section(value, index, 1);
                            connectTriple(section, rightPart, rightNeighbour);
                            putSection(rightPart);
                        }

                    } else {
                        // Интервал делится на три куска слева и справа со старым значением и в середине с новым
                        Section middlePart = new Section(value, index, 1);
                        int newIndex = index + 1;
                        checkIndex(newIndex);
                        Section rightPart =
                                new Section(section.value, newIndex, section.copies - index);
                        section.copies = index - section.index;
                        Section rightNeighbour = section.right;
                        connectPair(section, middlePart);
                        connectTriple(middlePart, rightPart, rightNeighbour);
                        putSection(middlePart);
                        putSection(rightPart);
                    }
                }
            } else {
                // Невлазим в диапазон левой секции
                Section rightNeighbour = section.right;
                // Проверим может мы пососедству с индексами левых или правых сегментов
                // с теми же значениями, в таких случаях можно сливаться
                if (section.isNextToRight(index) && value.equals(section.value)
                        && section != MIN_STUB) {
                    appendOneFromRight(section);
                } else if (rightNeighbour.isNextToLeft(index) && value.equals(rightNeighbour.value)
                        && rightNeighbour != MAX_STUB) {
                    appendOneFromLeft(rightNeighbour);
                } else {
                    // Значит и слева и справа промежутки пустых значений, нужно вставлять
                    Section newSection = new Section(value, index, 1);
                    connectTriple(section, newSection, rightNeighbour);
                    putSection(newSection);
                }
            }
        }

    }

    private void putSection(final Section newSection) {
        map.put(newSection.index, newSection);
    }

    private void removeSection(final Section newSection) {
        map.remove(newSection.index);
    }

    private void appendOneFromLeft(final Section section) {
        map.remove(section.index);
        section.copies++;
        section.index--;
        putSection(section);
    }

    private void appendOneFromRight(final Section section) {
        section.copies++;
    }

    private void connectPair(final Section leftSection, final Section rightSection) {
        leftSection.right = rightSection;
        rightSection.left = leftSection;
    }

    private void connectTriple(final Section leftSection, final Section newSection,
                               final Section rightSection) {
        leftSection.right = newSection;
        newSection.left = leftSection;
        newSection.right = rightSection;
        rightSection.left = newSection;
    }

    private void checkIndex(final int index) {
        if (index <= MIN_INDEX || index == MAX_INDEX) {
            throw new IndexOutOfBoundsException();
        }
    }

    public static void main(String[] args) {
        RLEList<String> list = new RLEListRefImpl<>();
        // Default state
        assertEq(null, list.get(0));
        assertEq(null, list.get(10));
        Iterator<String> iterator = list.iterator();
        assertEq(false, iterator.hasNext());
        boolean noSuchElementExceptionFound = false;
        try {
            iterator.next();
        } catch (NoSuchElementException e) {
            noSuchElementExceptionFound = true;
        }
        assertEq(true, noSuchElementExceptionFound);

        // One by one sequence insert
        list.append("A");
        assertEq("A", list.get(0));
        assertEq(null, list.get(1));
        list.append("A");
        assertEq("A", list.get(0));
        assertEq("A", list.get(1));
        assertEq(null, list.get(2));
        assertEq("AA", iteratorToString(list.iterator()));

        // Overwrite insert
        list.insert(0,"B");
        assertEq("B", list.get(0));
        assertEq("A", list.get(1));
        assertEq(null, list.get(2));
        assertEq("BA", iteratorToString(list.iterator()));
        list.insert(1,"B");
        assertEq("B", list.get(0));
        assertEq("B", list.get(1));
        assertEq(null, list.get(2));
        assertEq("BB", iteratorToString(list.iterator()));

        // Insert with skipped elements
        list.insert(3,"A");
        assertEq("B", list.get(0));
        assertEq("B", list.get(1));
        assertEq(null, list.get(2));
        assertEq("A", list.get(3));
        assertEq(null, list.get(4));
        assertEq("BBA", iteratorToString(list.iterator()));

        // Insert between segments left
        list = new RLEListRefImpl<>();
        list.insert(1,"A");
        list.insert(2,"A");
        list.insert(3,"A");
        list.insert(5,"B");
        list.insert(6, "B");
        assertEq("A", list.get(1));
        assertEq("A", list.get(2));
        assertEq("A", list.get(3));
        assertEq(null, list.get(4));
        assertEq("B", list.get(5));
        assertEq("B", list.get(6));
        assertEq("AAABB", iteratorToString(list.iterator()));
        list.insert(4, "A");
        assertEq("A", list.get(1));
        assertEq("A", list.get(2));
        assertEq("A", list.get(3));
        assertEq("A", list.get(4));
        assertEq("B", list.get(5));
        assertEq("B", list.get(6));
        assertEq("AAAABB", iteratorToString(list.iterator()));

        // Insert between segments right
        list = new RLEListRefImpl<>();
        list.insert(1,"A");
        list.insert(2,"A");
        list.insert(3,"A");
        list.insert(5,"B");
        list.insert(6, "B");
        assertEq("A", list.get(1));
        assertEq("A", list.get(2));
        assertEq("A", list.get(3));
        assertEq(null, list.get(4));
        assertEq("B", list.get(5));
        assertEq("B", list.get(6));
        assertEq("AAABB", iteratorToString(list.iterator()));
        list.insert(4, "B");
        assertEq("A", list.get(1));
        assertEq("A", list.get(2));
        assertEq("A", list.get(3));
        assertEq("B", list.get(4));
        assertEq("B", list.get(5));
        assertEq("B", list.get(6));
        assertEq("AAABBB", iteratorToString(list.iterator()));

        // Insert into segment
        list = new RLEListRefImpl<>();
        list.insert(1,"A");
        list.insert(2,"A");
        list.insert(3,"A");

        assertEq(null, list.get(0));
        assertEq("A", list.get(1));
        assertEq("A", list.get(2));
        assertEq("A", list.get(3));
        assertEq(null, list.get(4));
        assertEq("AAA", iteratorToString(list.iterator()));
        list.insert(2,"B");
        assertEq(null, list.get(0));
        assertEq("A", list.get(1));
        assertEq("B", list.get(2));
        assertEq("A", list.get(3));
        assertEq(null, list.get(4));
        assertEq("ABA", iteratorToString(list.iterator()));

        // iterator remove test
        list = new RLEListRefImpl<>();
        list.insert(1,"A");
        list.insert(2,"A");
        list.insert(3,"A");
        list.insert(4,"B");
        list.insert(5,"A");
        list.insert(6,"B");
        assertEq("AAABAB", iteratorToString(list.iterator()));
        Iterator<String> stringIterator = list.iterator();
        while (stringIterator.hasNext()){
            if(stringIterator.next().equals("A")){
                stringIterator.remove();
            }
        }
        assertEq(null, list.get(1));
        assertEq(null, list.get(2));
        assertEq(null, list.get(3));
        assertEq("B", list.get(4));
        assertEq(null, list.get(5));
        assertEq("B", list.get(6));
        assertEq("BB", iteratorToString(list.iterator()));


        System.out.println("Test was passed!");

    }

    private static String iteratorToString(Iterator<String> iterator) {
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next());
        }
        return sb.toString();
    }

    private static <V> void assertEq(V expected, V actual) {
        if (expected == actual) {
            return;
        }
        if (expected == null) {
            throw new RuntimeException("Test failed! Expected: null  but actual:" + actual);
        }
        if (actual == null || !expected.equals(actual)) {
            throw new RuntimeException("Test failed! Expected:" + expected + " but actual:"
                    + actual);
        }
    }

}