package com.bolsadeideas.springboot.reactor.app;

import com.bolsadeideas.springboot.reactor.app.model.Comentarios;
import com.bolsadeideas.springboot.reactor.app.model.Usuario;
import com.bolsadeideas.springboot.reactor.app.model.UsuarioComentarios;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SpringBootReactorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		//ejemploIterable();
		//ejemploFlatMap();
		//otroEjemplo();
		//ejemploToString();
		//ejemploConvertirCollectList();
		//ejemploUsuarioComentariosFlatMap();
		//ejemploUsuarioComentariosZipWith();
		//ejemploUsuarioComentariosZipWithForma2();
		//ejemploZipWithRange();
		//ejemploInterval();
		//ejemploDelayElements();
		//ejemploIntervalInfinite();
		//ejemploIntervalDesdeCreate();
		ejemploContrapresion();
	}

	public void ejemploContrapresion(){

		Flux.range(1,10)
				.log()
				.limitRate(5)//puede ser en vez de subscribe
				.subscribe(/*new Subscriber<Integer>() {
					private Subscription s;
					private Integer limite=5;
					private Integer consumido=0;
					@Override
					public void onSubscribe(Subscription s) {
							this.s=s;
							s.request(limite);
					}

					@Override
					public void onNext(Integer integer) {
							log.info(integer.toString());
						   consumido++;
						   if (consumido==limite){
							   consumido=0;
							   s.request(limite);
						   }
					}

					@Override
					public void onError(Throwable throwable) {

					}

					@Override
					public void onComplete() {

					}
				}*/);
	}

	public void ejemploIntervalDesdeCreate(){
		Flux.create(emitter->{
			Timer timer=new Timer();
			timer.schedule(new TimerTask() {
					private Integer contador=0;
					@Override
					public void run() {
						emitter.next(++contador);
						if (contador==10){
							timer.cancel();
							emitter.complete();
						}
						if (contador==5){
							timer.cancel();
							emitter.error(new InterruptedException("Error, se ha detenido el flux en 5!"));
						}
					}
				}, 1000, 1000);
		    })
				//.doOnNext(next->log.info(next.toString()))
				//.doOnComplete(()->log.info("Hemos terminado"))
				//.subscribe();
				.subscribe(next->log.info(next.toString()),error-> log.error(error.getMessage()),()->log.info("Hemos terminado"));
	}

	public void ejemploIntervalInfinite() throws InterruptedException {

		CountDownLatch latch=new CountDownLatch(1);//sirve como una forma de bloquear el hilo principal (generalmente el main) hasta que se complete cierto evento

		Flux.interval(Duration.ofSeconds(1))
				.doOnTerminate(latch::countDown)
				.flatMap(i->{
					if (i >= 5) {
					   return Flux.error(new InterruptedException("Solo hasta 5!"));
					}
					return Flux.just(i);
				})
				.map(i->"Hola "+i)
				//.doOnNext(log::info)
				.retry(2)
				.subscribe(log::info,e->log.error(e.getMessage()));

				latch.await();
	}

	public void ejemploDelayElements() throws InterruptedException {

		Flux<Integer> rango=Flux.range(1,12)
				.delayElements(Duration.ofSeconds(1))
				.doOnNext(i-> log.info(i.toString()));
		//rango.blockLast();
		rango.subscribe();
		Thread.sleep(13000);



	}

	public void ejemploInterval(){
		Flux<Integer> rango=Flux.range(1,12);
		Flux<Long> retraso=rango.interval(Duration.ofSeconds(1));

		rango.zipWith(retraso,(ra,re)->ra)
				.doOnNext(i-> log.info(i.toString()))
				.blockLast();//bloque el primero , no es recomendable pq ya no seria no bloqueante, en vez de eso usar subscribe()
	}

	public void ejemploZipWithRange(){
//		Flux.just(1,2,3,4)
//				.map(i->i*2)
//				.zipWith(Flux.range(0,4),(uno,dos)-> String.format("Primer Flux: %d, Segundo Flux: %d",uno,dos))
//				.subscribe(log::info);

		Flux<Integer> rangos=Flux.range(0,4);
		Flux.just(1,2,3,4)
				.map(i->i*2)
				.zipWith(rangos,(uno,dos)-> String.format("Primer Flux: %d, Segundo Flux: %d",uno,dos))
				.subscribe(log::info);
	}

	public void ejemploUsuarioComentariosZipWithForma2(){
		Mono<Usuario> usuarioMono=Mono.fromCallable(()-> new Usuario("John","Doe"));
		Mono<Comentarios>comentarioUsuarioMono=Mono.fromCallable(()->{
			Comentarios comentarios=new Comentarios();
			comentarios.addComentarios("Hola pepe, que tal");
			comentarios.addComentarios("Mañana voy a la playa");
			comentarios.addComentarios("Estoy tomando el curso de spring con reactor");
			return comentarios;
		});

		Mono<UsuarioComentarios> usuarioConComentarios=usuarioMono.zipWith(comentarioUsuarioMono)
				.map(tuple-> {
					Usuario u=tuple.getT1();
					Comentarios c=tuple.getT2();
					return new UsuarioComentarios(u,c);
				});
		usuarioConComentarios.subscribe(uc-> log.info(uc.toString()));
	}

	public void ejemploUsuarioComentariosZipWith(){
		Mono<Usuario> usuarioMono=Mono.fromCallable(()-> new Usuario("John","Doe"));
		Mono<Comentarios>comentarioUsuarioMono=Mono.fromCallable(()->{
			Comentarios comentarios=new Comentarios();
			comentarios.addComentarios("Hola pepe, que tal");
			comentarios.addComentarios("Mañana voy a la playa");
			comentarios.addComentarios("Estoy tomando el curso de spring con reactor");
			return comentarios;
		});

		Mono<UsuarioComentarios> usuarioConComentarios=usuarioMono
				.zipWith(comentarioUsuarioMono, (usuario,comentariosUsuario)->new UsuarioComentarios(usuario,comentariosUsuario));//usuarioMono.zipWith(comentarioUsuarioMono, UsuarioComentarios::new)

		usuarioConComentarios.subscribe(uc-> log.info(uc.toString()));
	}

	public void ejemploUsuarioComentariosFlatMap(){
		Mono<Usuario> usuarioMono=Mono.fromCallable(()-> new Usuario("John","Doe"));
		Mono<Comentarios>comentarioUsuarioMono=Mono.fromCallable(()->{
			Comentarios comentarios=new Comentarios();
			comentarios.addComentarios("Hola pepe, que tal");
			comentarios.addComentarios("Mañana voy a la playa");
			comentarios.addComentarios("Estoy tomando el curso de spring con reactor");
			return comentarios;
		});

	usuarioMono.flatMap(u-> comentarioUsuarioMono.map(c->new UsuarioComentarios(u,c)))
				.subscribe(uc-> log.info(uc.toString()));
	}

	public void ejemploConvertirCollectList() throws Exception {

		List<Usuario> usuariosList=new ArrayList<>();
		usuariosList.add(new Usuario("Andres"," Guzman"));
		usuariosList.add(new Usuario("Pedro","Fulano"));
		usuariosList.add(new Usuario("Maria","Fulana"));
		usuariosList.add(new Usuario("Diego","Sultano"));
		usuariosList.add(new Usuario("Juan","Mengano"));
		usuariosList.add(new Usuario("Bruce","Lee"));
		usuariosList.add(new Usuario("Bruce","Willis"));

		//Mono<List<Usuario>>lis=Flux.fromIterable(usuariosList).collectList();

	    Flux.fromIterable(usuariosList)
				.collectList()
		        .subscribe(/*lista -> log.info(lista.toString())*/
				  lista->lista.forEach(item->log.info(item.toString())));//System.out::println)
	}


	public void ejemploToString() throws Exception {

		List<Usuario> usuariosList=new ArrayList<>();
		usuariosList.add(new Usuario("Andres"," Guzman"));
		usuariosList.add(new Usuario("Pedro","Fulano"));
		usuariosList.add(new Usuario("Maria","Fulana"));
		usuariosList.add(new Usuario("Diego","Sultano"));
		usuariosList.add(new Usuario("Juan","Mengano"));
		usuariosList.add(new Usuario("Bruce","Lee"));
		usuariosList.add(new Usuario("Bruce","Willis"));

		Flux.fromIterable(usuariosList)
				.map(usuario -> usuario.getNombre().toUpperCase().concat(" ").concat(usuario.getApellido().toUpperCase()))
				.flatMap(nombre -> {
					if (nombre.contains("bruce".toUpperCase())){
						return Mono.just(nombre);
					}
					else{
						return Mono.empty();
					}
				})
				.map(String::toLowerCase)
				.subscribe(u -> log.info(u.toString()));
	}

	public void otroEjemplo(){
		List<Mono<String>> monos = List.of(
				Mono.just("bruce"),
				Mono.empty(),
				Mono.just("diana"),
				Mono.just("clark")
		);

		Function<Mono<String>,Mono<String>> listaMonos= new Function<Mono<String>, Mono<String>>() {
			@Override
			public Mono<String> apply(Mono<String> stringMono) {
				return stringMono;
			}
		};

		listaMonos.apply(Mono.just("Rommer"));

		Flux<String> resultado = Flux.fromIterable(monos)
				//.flatMap(mono -> mono); // Aquí se aplana todo
						.flatMap(listaMonos);

		resultado.subscribe(System.out::println);

	}

	public void ejemploFlatMap() throws Exception {

		List<String> usuariosList=new ArrayList<>();
		usuariosList.add("Andres Guzman");
		usuariosList.add("Pedro Fulano");
		usuariosList.add("Maria Fulana");
		usuariosList.add("Diego Sultano");
		usuariosList.add("Juan Mengano");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

	    Flux.fromIterable(usuariosList)
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.flatMap(usuario -> {
					if (usuario.getNombre().equalsIgnoreCase("bruce")){
						return Mono.just(usuario);
					}
					else{
						return Mono.empty();
					}
				})
				.map(usuario -> {
					String nombre = usuario.toString();
					usuario.setNombre(nombre);
					return usuario;
				})
				.subscribe(u -> log.info(u.toString()));
	}
	public void ejemploIterable() throws Exception {

		List<String> usuariosList=new ArrayList<>();
		usuariosList.add("Andres Guzman");
		usuariosList.add("Pedro Fulano");
		usuariosList.add("Maria Fulana");
		usuariosList.add("Diego Sultano");
		usuariosList.add("Juan Mengano");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

		Flux<String> nombres = Flux.fromIterable(usuariosList);//Flux.just("Andres Guzman", "Pedro Fulano", "Maria Fulana", "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis");
		//doOnNext(elemento->System.out.println(elemento))
		Flux<Usuario> usuarios = nombres.map(/*nombre-> {
					return nombre.toUpperCase();}*/
						//	String::toUpperCase
						nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase())
				)
				.filter(usuario -> usuario.getNombre().toLowerCase().equals("bruce"))
				.doOnNext(usuario -> {//Como se transformo a Usuario ya no es un flux de String sino de Usuario
					if (usuario == null) {
						throw new RuntimeException("Nombres no pueden ser vacios");
					}
					System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				}).map(usuario -> {
					String nombre = usuario.toString();//.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		//nombres.doOnSubscribe(subscription -> System.out.println("Suscripción iniciada...")).subscribe();
		usuarios.subscribe(e -> log.info(e.toString()), error -> log.error(error.getMessage()), new Runnable() {
			@Override
			public void run() {
				log.info("Ha finalizado la ejecución del observable con éxito");
			}
		});//si o si debe estar el subscribe para que se muestren los elementos, a pesar que haya doOnSubcribe	}
	}
}