/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.implementacao.informacao1;

/**
 *
 * @author maria.gabriele
 */
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Informacao2 {
    public static class MapperInformacao2 extends Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object have, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String [] campos = linha.split(";");


            if(campos.length == 10 && campos[0].equals("Brazil")){
                String pais = campos[0];
                int transacoes = 3;

                Text chaveMap = new Text(pais);
                IntWritable valorMap = new IntWritable(transacoes);

                context.write(chaveMap, valorMap);

            }

        }
    }
    public static class ReducerInformacao2 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            int soma = 0;
            for(IntWritable val : valores){
                soma = soma += val.get();
            }

            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);

        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_inteira.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/maria.gabriele/Desktop/ATP-Maria-Gabriele/Informacao2";

        if (args.length == 2 ){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf , "atividade1");

        job.setJarByClass(Informacao2.class);
        job.setMapperClass(MapperInformacao2.class);
        job.setReducerClass(ReducerInformacao2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }

}
