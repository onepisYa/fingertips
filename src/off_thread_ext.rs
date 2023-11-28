/*
 * Copyright (c) 2023 by onepisYa pis1@qq.com , All Rights Reserved.
 * @Date: 2023-11-13 15:54:41
 * @LastEditors: onepisYa pis1@qq.com
 * @LastEditTime: 2023-11-28 22:27:48
 * @FilePath: /fingertips/src/off_thread_ext.rs
 * 路漫漫其修远兮，吾将上下而求索。
 * @Description:
 */
 use std::io;
 use std::iter::FilterMap;
 
 use std::fmt::Debug;
 use std::sync::mpsc::{self, Sender};
 use std::sync::{Arc, Mutex};
 use std::thread;
 
 pub trait OffThreadExt: Iterator {
     /// 将这个迭代器转换为线程外迭代器：`next()`调用发生在
     /// 单独的工作线程上，因此该迭代器和循环体会同时运行
     fn off_thread(self) -> mpsc::IntoIter<Self::Item>;
     // fn off_thread_scan<St, B, F>(self, initial_state: St, f: F) -> Scan<Self, St, F>
     // where
     //     Self: Sized,
     //     F: FnMut(&mut St, Self::Item) -> Option<B>,
     // {
     //     Scan::new(self, initial_state, f)
     // }
 }
 
 impl<T> OffThreadExt for T
 where
     T: Iterator + Send + 'static,
     T::Item: Send + 'static,
 {
     fn off_thread(self) -> mpsc::IntoIter<Self::Item> {
         // 创建一个通道把条目从工作线程中传出去
         let (sender, receiver) = mpsc::sync_channel(1024);
 
         // 把这个迭代器转移给新的工作线程，并在那里运行它
         let _ = thread::spawn(move || {
             for item in self {
                 if sender.send(item).is_err() {
                     break;
                 }
             }
         })
         .join();
 
         // 返回一个从通道中拉取值的迭代器
         receiver.into_iter()
     }
 }
 
 // ---------------------
 
 type IsOk<I, T, R> = FilterMap<I, Box<dyn Fn(T) -> Option<R> + Send + Sync>>;
 // ! Box<dyn Fn(&T) -> bool> 是不可 Send 的类型
 // type IsOk<I, T> = Filter<I, impl Fn(&T) -> bool>;
 // ! 这个写法  impl trait 仅仅允许 在 unstale 的rust 中使用。
 // type IsOk<I, T> = Filter<I, fn(&T) -> bool>;
 // type IsOk<I, T> = Filter<I, |&T| -> bool>;
 
 pub trait ErrorsTo<T>: Iterator {
     fn errors_to(
         self,
         error_sender: Arc<Mutex<Sender<Result<T, std::io::Error>>>>,
     ) -> IsOk<Self, Self::Item, T>
     where
         Self: Sized;
 }
 
 // ? 有时候需要拆分为不同的 Trait 这样更清楚，对于类型来说限制上也会好处理一些。
 impl<I, T> ErrorsTo<T> for I
 where
     I: Iterator<Item = io::Result<T>> + Send + 'static,
     // * ✅ 重点是如何确定这个  Item 的类型
     // ？ 因为是处理错误 所以这里面肯定是 Result，不过具体是普通的还是做了别名的 Result 是要看具体情况
     T: Send + 'static + Sync,
 {
     fn errors_to(
         self,
         error_sender: Arc<Mutex<Sender<Result<T, std::io::Error>>>>,
     ) -> IsOk<Self, Self::Item, T> {
         // * 迭代器克隆开销很便宜
         // ? 除非你使用了 move 这种，然后在闭包中使用了 捕获的一些数据，那么将会导致迭代器的克隆开销很大
         let _ = thread::spawn(move || {
 
         });
         let o: IsOk<Self, Self::Item, T> = self.filter_map(Box::new(move |item| {
             if item.is_err() {
                 let _ = error_sender.lock().unwrap().send(item).is_ok();
                 None
             } else {
                 Some(item.unwrap())
             }
         }));
         o
     }
 }
 
 // -------------
 
 // 定义一个新的迭代器类型
 #[derive(Debug)]
 pub enum AccValue<T> {
     Empty,
     Value(T),
 }
 
 // 定义一个 trait 来扩展标准库迭代器
 pub trait ConditionAccmulatorExt<T>: Iterator<Item = T> {
     fn condition_reduce<F>(self, func: F) -> ConditionAccmulator<Self, F, T>
     where
         Self: Sized,
         F: FnMut(&mut T, Option<T>) -> Option<T>;
     // F: Fn(T) -> bool;
 }
 
 // 定义一个新的迭代器类型
 pub struct ConditionAccmulator<I: Iterator<Item = T>, F, T> {
     iterator: I,
     acc: T,             // 当前累积的值
     done: bool,         // 是否完成累积
     current: Option<T>, // 当前元素的值
     condition_func: F,  // 条件函数
 }
 
 // 实现 AccumulateNumbersIterator 的迭代器接口
 impl<I: Iterator<Item = T>, F, T: Debug + Default + Clone> Iterator for ConditionAccmulator<I, F, T>
 where
     F: FnMut(&mut T, Option<T>) -> Option<T>,
 {
     type Item = AccValue<T>;
 
     fn next(&mut self) -> Option<Self::Item> {
         if self.done {
             // 如果已经完成累积，则返回 None
             return None;
         }
 
         // 从迭代器中获取下一个元素
         let next = match self.current.take() {
             Some(num) => Some(num),
             None => self.iterator.next(),
         };
 
         let condition_item = match next {
             Some(val) => {
                 if let Some(next_val) = self.iterator.next() {
                     self.current = Some(next_val);
                 } else {
                     self.done = true;
                 };
                 Some(val)
             }
             None => {
                 self.done = true;
                 None
             }
         };
         let condition_result = (self.condition_func)(&mut self.acc, condition_item);
         let result = match condition_result {
             Some(result) => {
                 print!("result: {:?}\n", result);
                 AccValue::Value(result)
             }
             None if self.done == false => AccValue::Empty,
             // None => AccValue::Value(self.acc.clone()),
             None => {
                 let old_acc = std::mem::replace(&mut self.acc, T::default());
                 AccValue::Value(old_acc)
             }
         };
 
         Some(result)
     }
 }
 
 // 为迭代器实现自定义的 trait
 impl<I: Iterator<Item = T>, T: Default> ConditionAccmulatorExt<T> for I {
     fn condition_reduce<F>(self, func: F) -> ConditionAccmulator<Self, F, T>
     where
         Self: Sized,
         // F: Fn(T) -> bool,
         F: FnMut(&mut T, Option<T>) -> Option<T>,
     {
         ConditionAccmulator {
             iterator: self,
             acc: T::default(),
             done: false,
             current: None,
             condition_func: func,
         }
     }
 }
 
 #[allow(dead_code)] // 允许未使用的变量
 fn test_util(numbers: Vec<u32>, expected: Vec<u32>) {
     let result = numbers
         .into_iter()
         .condition_reduce(|acc, next| {
             println!("context {:?}", acc);
             *acc += next.unwrap();
             // INFO:　这段我们可以自定义合并的逻辑
             if *acc >= 10 {
                 // 如果满足条件，则返回累积值并重置累积值
                 let result = acc.clone();
                 // 重置  acc
                 *acc = 0;
                 Some(result)
             } else {
                 // 否则返回 None
                 None
             }
         })
         .filter_map(|x| match x {
             AccValue::Value(v) => Some(v),
             _ => None,
         })
         .collect::<Vec<u32>>();
     assert_eq!(result, expected);
 }
 
 #[cfg(test)]
 mod tests {
     use super::*;
 
     #[test]
     fn test_one() {
         let numbers = vec![1, 5, 3, 1, 10];
         test_util(numbers, vec![10, 10]);
         let numbers = vec![1, 5, 3, 1, 2, 10];
         test_util(numbers, vec![10, 12]);
         let numbers = vec![1, 5];
         test_util(numbers, vec![6]);
     }
 }
 