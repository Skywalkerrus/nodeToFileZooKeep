package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static ArrayList<String> addNodesToScope(String node)
    {
        ArrayList<String> nodeScope = new ArrayList<String>();
        String forAdd[] = node.split(", ");
        nodeScope.addAll(Arrays.stream(forAdd).toList());
        System.out.println("Scope Node: " + nodeScope);
        return  nodeScope;
    }

    public static void checkOrCreateNode(ArrayList<String> nodeScope, ZooKeeper zk) throws InterruptedException, KeeperException, IOException {
        //Stat stat = zookeeper.exists(path, false);
        StringBuffer buffNode = new StringBuffer();
        for (String node1 : nodeScope)
        {

            String[] ndd = node1.split("/");
            ArrayList<String> list = new ArrayList<>(Arrays.asList(ndd));
            if (!list.isEmpty()) {
                list.remove(0);
            }
            ndd = list.toArray(new String[0]);//при разделении нод на отдельные ячейки строкового массива,
            // первым элементом отделяется слэш, поэтому тут его удаляем

            int i = 0;
            buffNode.delete(0, buffNode.length()); // очистка нужна чтоб следующие узлы не слепливались с предыдущими
            System.out.println("NDD---> " + Arrays.stream(ndd).toList());
            while (i < ndd.length) {
                buffNode.append("/").append(ndd[i]); // лепим один узел к другому, тк изначально их распилили для
                // проверки по каждому узлу, существует ли он. И если его не существует, то надо его создать
                // Но, если узла, например, три, то сначала надо создать корневой узел, потом создать узел со вторым узлом
                // и уже после этого приклеивать третий узел на предыдущие 2, которые создаются по отдельности, по цепочке.
                Stat stat = new Stat();
                try {
                    System.out.println("NODE!!!! ->" + node1);
                    zk.getData("/" + ndd[i], true, null);
                } catch (KeeperException.NoNodeException e) {

                    LOG.error("Node {} does not exist!", ndd[i]);
                    System.out.println("Try create NODE->/" + ndd[i] + " with data->" + "dataBy" + ndd[i] + "\nBuffNode: " + buffNode);
                    createNode(zk, buffNode.toString(), "dataBy" + ndd[i]);
                }
                i++;
            }
            System.out.println("NODE With --->" + node1 + " BUFFNODE --->" + buffNode);
            // проверка на родителя/ребенка и создание файла/папки
            readAndSaveNodesLikeFiles(zk, node1, "dirForNodes");
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String strNode = "/node1, /node2/node3, /node4, /node5/node6/node7, /node5/node6/node8, /node5/node6/node7/node8";
        // перечисление обязательно через запятую с пробелом
        ArrayList <String> scopeNode = addNodesToScope(strNode); // список нод, которые надо создать в зоокипере, если их там нет

        ZooKeeper zk = new ZooKeeper("localhost:2181", 45000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
            }
        });
        checkOrCreateNode(scopeNode, zk); // проверяем наличие нод из списка
    }

    public static void createNode(ZooKeeper zk, String nodeName, String nodeData) throws KeeperException, InterruptedException {
        if (zk.exists(nodeName, false) == null){
            zk.create(nodeName, nodeData.getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
        }
    }

    private static void readAndSaveNodesLikeFiles(ZooKeeper zk, String path, String saveDirectory) throws KeeperException, InterruptedException, IOException {
        // Проверяем, существует ли узел
        Stat stat = zk.exists(path, false);

        //  /node1, /node2/node3, /node4, /node5/node6/node7, /node5/node6/node8
        // 1, 3, 4, 7, 8 - files
        // 2, 5, 6 - dirs
        // алгоритм заключается в том, что если дальше будет следующая итерация,
        // мы создаем папку, а если итерации не будет, создается файл.

        if (stat != null) {

            String[] fullPathSplit = path.split("/");
            ArrayList<String> list = new ArrayList<>(Arrays.asList(fullPathSplit));
            if (!list.isEmpty()) {
                list.remove(0);
            }
            fullPathSplit = list.toArray(new String[0]); //при разделении нод на отдельные ячейки строкового массива,
            // первым элементом отделяется слэш, поэтому тут его удаляем

            int lenPath = fullPathSplit.length;
            File dir = new File(saveDirectory);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File currentDir = dir;
            for (int ji = 0; ji < lenPath; ji++) { //цикл для прохождения по всем узлам нод
                if ((ji + 1) < lenPath) { // с помощью замера массива можно понять, сколько будет итераций.
                    // Если итерации дальше есть, значит, это еще не конечный узел и его делаем папкой
                    // Создаем папку для текущего узла если дальше есть дочка
                    File childDir = new File(currentDir, fullPathSplit[ji]);
                    if (!childDir.exists()) {
                        childDir.mkdirs();
                    }
                    currentDir = childDir;
                } else { // сюда заходит, когда это последняя итерация, а соответственно и последняя нода, ее делаем файлом
                    File file = new File(currentDir, fullPathSplit[ji] + ".txt");
                    try (FileWriter writer = new FileWriter(file)) {
                        String data = "";
                        System.out.println("Path: " + path);
                            data = new String(zk.getData(path, false, null));
                        System.out.println("File CREATED: " + file.getAbsolutePath());
                        writer.write(data);
                    }
                }
            }
        }
    }
}