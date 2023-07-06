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

public class Informacao4 {
    public static class MapperInformacao4 extends Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object have, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String [] campos = linha.split(";");
            if(campos.length == 10){
                String ano = campos[1];
                int transacoes = 1;

                Text chaveMap = new Text(ano);
                IntWritable valorMap = new IntWritable(transacoes);

                context.write(chaveMap, valorMap);

            }

        }
    }
    public static class ReducerInformacao4 extends Reducer<Text, IntWritable, Text, IntWritable>{
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
        String arquivoSaida = "/home2/ead2022/SEM1/maria.gabriele/Desktop/ATP-Maria-Gabriele/Informacao3";

        if (args.length == 2 ){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf , "atividade4");

        job.setJarByClass(Informacao3.class);
        job.setMapperClass(MapperInformacao4.class);
        job.setReducerClass(ReducerInformaca43.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }

}
