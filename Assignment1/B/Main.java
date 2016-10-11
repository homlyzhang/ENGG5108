import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final int BASKET_NUM = 9999;
    private static final int THRESHOLD = 50;

    public static void main(String[] args) {
        List<List<Integer>> frequentItemsets = new ArrayList<>();

        List<Integer> primeNumbers = getPrimeNumbers();

        List<List<Integer>> tempItemsets = new ArrayList<>();
        primeNumbers.forEach(p -> {
            List<Integer> itemset = new ArrayList<>();
            itemset.add(p);
            tempItemsets.add(itemset);
        });
        List<List<Integer>> levelFrequentItemsets = getTrulyFrequentItemsets(tempItemsets);

        List<Integer> frequentPrimeNumbers = new ArrayList<>();
        levelFrequentItemsets.forEach(i -> i.forEach(frequentPrimeNumbers :: add));
        levelFrequentItemsets = getNextLevelFrequentItemsets(levelFrequentItemsets, frequentPrimeNumbers);

        do {
            levelFrequentItemsets = getNextLevelFrequentItemsets(levelFrequentItemsets, frequentPrimeNumbers);
            frequentItemsets.addAll(levelFrequentItemsets);
        } while (levelFrequentItemsets.size() > 0);

        frequentItemsets.forEach(itemset -> {
            itemset.forEach(item -> System.out.print(item + " "));
            System.out.println();
        });
    }

    private static List<Integer> getPrimeNumbers() {
        List<Integer> primeNumbers = new ArrayList<>();
        for (int i = 2; i < BASKET_NUM; i ++) {
            boolean isPrime = true;
            for (int j = 2; j < i - 1; j ++) {
                if (i % j == 0) {
                    isPrime = false;
                    break;
                }
            }
            if (isPrime) {
                primeNumbers.add(i);
            }
        }
        return primeNumbers;
    }

    private static List<List<Integer>> getNextLevelFrequentItemsets(List<List<Integer>> lastLevelFrequentItemsets,
                                                                    List<Integer> frequentPrimeNumbers) {
        List<List<Integer>> tempItemsets = new ArrayList<>();
        lastLevelFrequentItemsets.forEach(itemset -> {
            Integer lastPrimeNumber = itemset.get(itemset.size() - 1);
            frequentPrimeNumbers.forEach(primeNumber -> {
                if (primeNumber > lastPrimeNumber) {
                    List<Integer> tempItemset = new ArrayList<>();
                    tempItemset.addAll(itemset);
                    tempItemset.add(primeNumber);
                    tempItemsets.add(tempItemset);
                }
            });
        });
        return getTrulyFrequentItemsets(tempItemsets);
    }

    private static List<List<Integer>> getTrulyFrequentItemsets(List<List<Integer>> assumeFrequentItemsets) {
        List<List<Integer>> trulyFrequentItemsets = new ArrayList<>();
        assumeFrequentItemsets.forEach(itemset -> {
            int tPrime = 1;
            for (int j : itemset.toArray(new Integer[itemset.size()])) {
                tPrime *= j;
            }
            int support = 0;
            for (int j = tPrime; j < BASKET_NUM; j++) {
                if (j % tPrime == 0) {
                    support++;
                }
            }
            if (support >= THRESHOLD) {
                trulyFrequentItemsets.add(itemset);
            }
        });
        return trulyFrequentItemsets;
    }
}
